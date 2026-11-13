// Copyright 2025 PingCAP, Inc.
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

#include <Common/FailPoint.h>
#include <Interpreters/Settings.h>
#include <Storages/KVStore/ProxyStateMachine.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ext/scope_guard.h>

// TODO: Move ServerInfo into KVStore, to make it more conhensive.
namespace DB
{
namespace FailPoints
{
extern const char force_set_proxy_state_machine_cpu_cores[];
} // namespace FailPoints

namespace tests
{
TEST(ProxyStateMachineTest, SetLogicalCores)
{
    {
        FailPointHelper::enableFailPoint(FailPoints::force_set_proxy_state_machine_cpu_cores);
        SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_set_proxy_state_machine_cpu_cores); });
        Settings settings;
        ServerInfo server_info;
        ProxyStateMachine proxy_machine{DB::Logger::get(), TiFlashProxyConfig::genForTest()};
        proxy_machine.getServerInfo(server_info, settings);
        ASSERT_EQ(settings.max_threads.get(), 12345);
    }
    {
        // If user explicitly set `max_threads`, then `getServerInfo` won't overwrite the value
        FailPointHelper::enableFailPoint(FailPoints::force_set_proxy_state_machine_cpu_cores);
        SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_set_proxy_state_machine_cpu_cores); });
        Settings settings;
        settings.max_threads.set(8);
        ServerInfo server_info;
        ProxyStateMachine proxy_machine{DB::Logger::get(), TiFlashProxyConfig::genForTest()};
        proxy_machine.getServerInfo(server_info, settings);
        ASSERT_EQ(settings.max_threads.get(), 8);
    }
    {
        Settings settings;
        ServerInfo server_info;
        ProxyStateMachine proxy_machine{DB::Logger::get(), TiFlashProxyConfig::genForTest()};
        proxy_machine.getServerInfo(server_info, settings);
        ASSERT_EQ(settings.max_threads.get(), std::thread::hardware_concurrency());
    }
}

namespace
{
/// The arguments that proxy is started with, each argument followed by its value.
std::vector<String> getProxyArgs(const String & toml, DisaggregatedMode mode, bool use_columnar)
{
    Settings settings;
    auto config = loadConfigFromString(toml);
    TiFlashProxyConfig proxy_config( //
        *config,
        mode,
        /*use_autoscaler=*/false,
        use_columnar,
        STORAGE_FORMAT_CURRENT,
        settings,
        DB::Logger::get());

    const auto args = proxy_config.getArgs();
    return {args.begin(), args.end()};
}

String findProxyArg(const std::vector<String> & args, const String & key)
{
    for (size_t i = 0; i + 1 < args.size(); ++i)
    {
        if (key == args[i])
            return args[i + 1];
    }
    return {};
}
} // namespace

TEST(ProxyStateMachineTest, ProxyKeyspaceLabel)
{
    const String raft_config = R"(
[raft]
pd_addr = "pd0:2379"
)";

    // A node bound to a keyspace reports the label to PD, so that TiDB is able to tell
    // which keyspace this node is allowed to serve.
    {
        const auto args = getProxyArgs(
            raft_config + R"(
[flash]
keyspace_name = "KS1"
)",
            DisaggregatedMode::Compute,
            /*use_columnar=*/true);
        ASSERT_EQ(findProxyArg(args, "--labels"), "keyspace=KS1,exclusive=no-data");
    }
    // Without `flash.keyspace_name`, no keyspace label is reported, so the node is shared
    // by all the keyspaces, which is the behavior before the label is introduced.
    {
        const auto args = getProxyArgs(raft_config, DisaggregatedMode::Compute, /*use_columnar=*/true);
        ASSERT_EQ(findProxyArg(args, "--labels"), "exclusive=no-data");
    }
    // An explicitly empty `flash.keyspace_name` is treated as if it were not set.
    {
        const auto args = getProxyArgs(
            raft_config + R"(
[flash]
keyspace_name = ""
)",
            DisaggregatedMode::Compute,
            /*use_columnar=*/true);
        ASSERT_EQ(findProxyArg(args, "--labels"), "exclusive=no-data");
    }
    // Labels configured by `flash.proxy.labels` are kept, and the keyspace label is
    // placed before them.
    {
        const auto args = getProxyArgs(
            raft_config + R"(
[flash]
keyspace_name = "KS1"

[flash.proxy]
labels = "zone=z1"
)",
            DisaggregatedMode::Compute,
            /*use_columnar=*/true);
        ASSERT_EQ(findProxyArg(args, "--labels"), "keyspace=KS1,zone=z1,exclusive=no-data");
    }
    // The proxy keeps the first occurrence of a duplicated label key from `--labels`,
    // so a `keyspace` label hand-written in `flash.proxy.labels` is overridden by
    // `flash.keyspace_name`.
    {
        const auto args = getProxyArgs(
            raft_config + R"(
[flash]
keyspace_name = "KS2"

[flash.proxy]
labels = "keyspace=KS1"
)",
            DisaggregatedMode::Compute,
            /*use_columnar=*/true);
        ASSERT_EQ(findProxyArg(args, "--labels"), "keyspace=KS2,keyspace=KS1,exclusive=no-data");
    }
    // A keyspace label makes sense for a write node as well.
    {
        const auto args = getProxyArgs(
            raft_config + R"(
[flash]
keyspace_name = "KS1"
)",
            DisaggregatedMode::Storage,
            /*use_columnar=*/false);
        ASSERT_EQ(findProxyArg(args, "--labels"), "keyspace=KS1,engine_role=write");
    }
    // Without any extra label, the configured labels are passed through as they are,
    // instead of getting a trailing comma.
    {
        const auto args = getProxyArgs(
            raft_config + R"(
[flash.proxy]
labels = "zone=z1"
)",
            DisaggregatedMode::None,
            /*use_columnar=*/false);
        ASSERT_EQ(findProxyArg(args, "--labels"), "zone=z1");
    }
    // An empty `flash.proxy.labels` does not produce a valueless `--labels` argument.
    {
        const auto args = getProxyArgs(
            raft_config + R"(
[flash.proxy]
labels = ""
)",
            DisaggregatedMode::None,
            /*use_columnar=*/false);
        ASSERT_EQ(findProxyArg(args, "--labels"), "");
    }
    // Without any label at all, `--labels` is not passed to proxy.
    {
        const auto args = getProxyArgs(raft_config, DisaggregatedMode::None, /*use_columnar=*/false);
        ASSERT_EQ(findProxyArg(args, "--labels"), "");
    }
}

TEST(ProxyStateMachineTest, ProxyKeyspaceNameValidation)
{
    // An invalid `flash.keyspace_name` fails the startup no matter whether the proxy is
    // going to be started: the value is validated before `raft.pd_addr` is even looked
    // at, so the typo is also rejected when the proxy is not started at all.
    const auto expect_invalid = [](const String & toml) {
        Settings settings;
        auto config = loadConfigFromString(toml);
        try
        {
            TiFlashProxyConfig proxy_config( //
                *config,
                DisaggregatedMode::None,
                /*use_autoscaler=*/false,
                /*use_columnar=*/false,
                STORAGE_FORMAT_CURRENT,
                settings,
                DB::Logger::get());
            FAIL() << "unreachable";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    };

    // A space is not a legal keyspace name character.
    expect_invalid(R"(
[flash]
keyspace_name = "KS 1"
)");
    // Longer than the keyspace name length limit of PD.
    expect_invalid(R"(
[flash]
keyspace_name = "012345678901234567890"
)");

    // A valid `flash.keyspace_name` without `raft.pd_addr` does not fail the startup,
    // the proxy is simply not started and no label is reported.
    {
        const auto args = getProxyArgs(
            R"(
[flash]
keyspace_name = "KS1"
)",
            DisaggregatedMode::None,
            /*use_columnar=*/false);
        ASSERT_EQ(findProxyArg(args, "--labels"), "");
    }
}
} // namespace tests
} // namespace DB
