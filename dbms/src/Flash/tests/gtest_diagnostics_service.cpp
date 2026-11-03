// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/TiFlashSecurity.h>
#include <Flash/DiagnosticsService.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/TemporaryFile.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <utility>
#include <vector>

namespace DB::tests
{
class DiagnosticsServiceTest : public ::testing::Test
{
protected:
    static String readFile(const std::filesystem::path & path)
    {
        std::ifstream input(path);
        input.exceptions(std::ios::failbit | std::ios::badbit);
        return String(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }

    auto makeConfig(const String & allowedCommonNames) const
    {
        return loadConfigFromString(fmt::format(
            R"(
[security]
ca_path = "{}"
cert_path = "{}"
key_path = "{}"
cert_allowed_cn = "{}"
[logger]
log = "{}/tiflash.log"
)",
            (certDirectory / "ca.crt").string(),
            (certDirectory / "cert.crt").string(),
            (certDirectory / "key.pem").string(),
            allowedCommonNames,
            logDirectory));
    }

    void SetUp() override
    {
        certDirectory = (std::filesystem::path(__FILE__).parent_path() / "../../Common/tests/tls").lexically_normal();
        logDirectory = Poco::TemporaryFile::tempName();
        Poco::File(logDirectory).createDirectory();
        {
            std::ofstream output(logDirectory + "/tiflash.log");
            output << "[2020/04/23 13:11:02.329 +08:00] [INFO] [\"diagnostics test message\"]\n";
        }

        context = Context::createGlobal(Context::ApplicationType::LOCAL);
        config = makeConfig("allowed-client");
        context->setSecurityConfig(*config, Logger::get());
        service = std::make_unique<DiagnosticsService>(*context, *config);

        grpc::SslServerCredentialsOptions serverOptions(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
        serverOptions.pem_root_certs = readFile(certDirectory / "ca.crt");
        serverOptions.pem_key_cert_pairs.push_back(
            {readFile(certDirectory / "key.pem"), readFile(certDirectory / "cert.crt")});
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::SslServerCredentials(serverOptions), &port);
        builder.RegisterService(service.get());
        server = builder.BuildAndStart();
        ASSERT_NE(server, nullptr);
        ASSERT_GT(port, 0);

        // The existing test certificate supports both server and client authentication.
        grpc::SslCredentialsOptions clientOptions;
        clientOptions.pem_root_certs = serverOptions.pem_root_certs;
        clientOptions.pem_private_key = serverOptions.pem_key_cert_pairs[0].private_key;
        clientOptions.pem_cert_chain = serverOptions.pem_key_cert_pairs[0].cert_chain;
        grpc::ChannelArguments arguments;
        arguments.SetSslTargetNameOverride("test.test.google.fr");
        stub = diagnosticspb::Diagnostics::NewStub(grpc::CreateCustomChannel(
            fmt::format("127.0.0.1:{}", port),
            grpc::SslCredentials(clientOptions),
            arguments));
    }

    void TearDown() override
    {
        if (server)
            server->Shutdown();
        if (!logDirectory.empty() && Poco::File(logDirectory).exists())
            Poco::File(logDirectory).remove(true);
    }

    void checkRpcAccess(bool allowed)
    {
        grpc::ClientContext logContext;
        logContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        diagnosticspb::SearchLogRequest logRequest;
        auto reader = stub->search_log(&logContext, logRequest);
        diagnosticspb::SearchLogResponse logResponse;
        size_t messageCount = 0;
        while (reader->Read(&logResponse))
        {
            for (const auto & message : logResponse.messages())
            {
                EXPECT_EQ(message.message(), R"(["diagnostics test message"])");
                ++messageCount;
            }
        }
        const auto logStatus = reader->Finish();
        EXPECT_EQ(logStatus.error_code(), allowed ? grpc::StatusCode::OK : grpc::StatusCode::PERMISSION_DENIED)
            << logStatus.error_message();
        EXPECT_EQ(messageCount, allowed ? 1 : 0);

        grpc::ClientContext infoContext;
        infoContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        diagnosticspb::ServerInfoRequest infoRequest;
        diagnosticspb::ServerInfoResponse infoResponse;
        const auto infoStatus = stub->server_info(&infoContext, infoRequest, &infoResponse);
        // This isolated context has no proxy; authorized requests reach the existing INTERNAL error path.
        EXPECT_EQ(infoStatus.error_code(), allowed ? grpc::StatusCode::INTERNAL : grpc::StatusCode::PERMISSION_DENIED)
            << infoStatus.error_message();
        EXPECT_EQ(infoResponse.items_size(), 0);
    }

    std::filesystem::path certDirectory;
    String logDirectory;
    std::unique_ptr<Context> context;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> config;
    std::unique_ptr<DiagnosticsService> service;
    std::unique_ptr<grpc::Server> server;
    std::unique_ptr<diagnosticspb::Diagnostics::Stub> stub;
};

TEST_F(DiagnosticsServiceTest, CommonNameAllowList)
{
    // Keep the same authenticated TLS channel while reloading the allowed CNs.
    for (const auto & [commonNames, allowed] : std::vector<std::pair<String, bool>>{
             {"allowed-client", false},
             {"*.test.google.com", true},
             {"other-client", false},
             {"", true},
         })
    {
        SCOPED_TRACE(commonNames);
        auto updatedConfig = makeConfig(commonNames);
        context->getSecurityConfig()->update(*updatedConfig);
        checkRpcAccess(allowed);
    }
}
} // namespace DB::tests
