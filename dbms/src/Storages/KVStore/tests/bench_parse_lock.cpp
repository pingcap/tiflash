// Copyright 2024 PingCAP, Inc.
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

#include <Common/typeid_cast.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TiKVHelpers/DecodedLockCFValue.h>
#include <benchmark/benchmark.h>

#include <random>

using namespace DB;

namespace DB::tests
{
using DB::RecordKVFormat::DecodedLockCFValue;

DecodedLockCFValue::Inner * decodeLockCfValue(const DecodedLockCFValue & decoded);

void parseTest(benchmark::State & state)
{
    try
    {
        std::string shor_value = "value";
        auto lock_for_update_ts = 7777, txn_size = 1;
        const std::vector<std::string> & async_commit = {"s1", "s2"};
        const std::vector<uint64_t> & rollback = {3, 4};
        auto lock_value2 = encodeFullLockCfValue(
            Region::DelFlag,
            "primary key",
            421321,
            std::numeric_limits<UInt64>::max(),
            &shor_value,
            66666,
            lock_for_update_ts,
            txn_size,
            async_commit,
            rollback,
            1111);

        auto ori_key = std::make_shared<const TiKVKey>(RecordKVFormat::genKey(1, 88888));
        for (auto _ : state)
        {
            auto lock2 = RecordKVFormat::DecodedLockCFValue(
                ori_key,
                std::make_shared<TiKVValue>(TiKVValue::copyFrom(lock_value2)));
            benchmark::DoNotOptimize(lock2);
        }
    }
    catch (...)
    {
        tryLogCurrentException(DB::Logger::get(), __PRETTY_FUNCTION__);
    }
}

BENCHMARK(parseTest);

} // namespace DB::tests
