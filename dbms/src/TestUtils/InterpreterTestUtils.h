// Copyright 2022 PingCAP, Ltd.
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

#pragma once

#include <Common/FmtUtils.h>
#include <Debug/MockExecutor.h>
#include <Debug/SerializeExecutor.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "Interpreters/Settings.h"
namespace DB
{
namespace tests
{
template <typename ExpectedT, typename ActualT, typename ExpectedDisplayT, typename ActualDisplayT>
::testing::AssertionResult assertEqual(
    const char * expected_expr,
    const char * actual_expr,
    const ExpectedT & expected_v,
    const ActualT & actual_v,
    const ExpectedDisplayT & expected_display,
    const ActualDisplayT & actual_display,
    const String & title = "")
{
    if (expected_v != actual_v)
    {
        auto expected_str = fmt::format("\n{}: {}", expected_expr, expected_display);
        auto actual_str = fmt::format("\n{}: {}", actual_expr, actual_display);
        return ::testing::AssertionFailure() << title << expected_str << actual_str;
    }
    return ::testing::AssertionSuccess();
}


#define ASSERT_EQUAL_WITH_TEXT(expected_value, actual_value, title, expected_display, actual_display)                                             \
    do                                                                                                                                            \
    {                                                                                                                                             \
        auto result = assertEqual(#expected_value, #actual_value, (expected_value), (actual_value), (expected_display), (actual_display), title); \
        if (!result)                                                                                                                              \
            return result;                                                                                                                        \
    } while (false)

#define ASSERT_EQUAL(expected_value, actual_value, title)                                                             \
    do                                                                                                                \
    {                                                                                                                 \
        auto expected_v = (expected_value);                                                                           \
        auto actual_v = (actual_value);                                                                               \
        auto result = assertEqual(#expected_value, #actual_value, expected_v, actual_v, expected_v, actual_v, title); \
        if (!result)                                                                                                  \
            return result;                                                                                            \
    } while (false)

class InterpreterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeDAGContext();
        initializeClientInfo();
    }

public:
    static void SetUpTestCase()
    {
        try
        {
            DB::registerFunctions(); // todo ywq figure out if it's needed.
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    }
    InterpreterTest()
        : context(TiFlashTestEnv::getContext())
    {}

    virtual void initializeDAGContext()
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        context.setDAGContext(dag_context_ptr.get());
    }

    virtual void initializeClientInfo()
    {
        /// Set a bunch of client information.
        // std::string user = getClientMetaVarWithDefault(grpc_context, "user", "default");
        // std::string password = getClientMetaVarWithDefault(grpc_context, "password", "");
        // std::string quota_key = getClientMetaVarWithDefault(grpc_context, "quota_key", "");
        // std::string peer = grpc_context->peer();
        // Int64 pos = peer.find(':');
        // if (pos == -1)
        // {
        //     return std::make_tuple(context, ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid peer address: " + peer));
        // }
        // std::string client_ip = peer.substr(pos + 1);
        // Poco::Net::SocketAddress client_address(client_ip);

        // context->setUser(user, password, client_address, quota_key);

        // String query_id = getClientMetaVarWithDefault(grpc_context, "query_id", "");
        context.setCurrentQueryId("test");

        ClientInfo & client_info = context.getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;
    }


    DAGContext & getDAGContext()
    {
        assert(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

    static String & trim(String & str)
    {
        if (str.empty())
        {
            return str;
        }

        str.erase(0, str.find_first_not_of(' '));
        str.erase(str.find_last_not_of(' ') + 1);
        return str;
    }

    ::testing::AssertionResult dagRequestEqual(
        const std::shared_ptr<tipb::DAGRequest> & expected,
        const std::shared_ptr<tipb::DAGRequest> & actual)
    {
        FmtBuffer fmt_buf;
        auto serialize = SerializeExecutor(context, fmt_buf);
        String expected_string = serialize.serialize(expected.get());
        fmt_buf.clear();
        String actual_string = serialize.serialize(actual.get());
        ASSERT_EQUAL(trim(expected_string), trim(actual_string), "DAGRequest mismatch");
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult dagRequestEqual(
        String & expected,
        const std::shared_ptr<tipb::DAGRequest> & actual)
    {
        FmtBuffer fmt_buf;
        auto serialize = SerializeExecutor(context, fmt_buf);
        String actual_string = serialize.serialize(actual.get());
        std::cout << actual_string << std::endl;
        std::cout << expected << std::endl;
        ASSERT_EQUAL(trim(expected), trim(actual_string), "DAGRequest mismatch");
        return ::testing::AssertionSuccess();
    }


protected:
    Context context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

} // namespace tests
} // namespace DB
