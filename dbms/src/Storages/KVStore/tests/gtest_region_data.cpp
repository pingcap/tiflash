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

#include <Common/BackgroundTask.h>
#include <Common/formatReadable.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/KVStore/MultiRaft/RegionData.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyValue.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class RegionDataTest : public ::testing::Test
{
public:
    RegionDataTest()
        : log(Logger::get())
    {}

    LoggerPtr log;
};

#if 0
TEST_F(RegionDataTest, InsertManyLocks)
{
    double real_rss;
    Int64 proc_thrds = 0;
    UInt64 vitr_size = 0;
    process_mem_usage(real_rss, proc_thrds, vitr_size);

    Int64 real_rss_before_insert = real_rss;

    RegionData reg_data;

    size_t test_num = 15'000'000UL;
    for (size_t i = 0; i < test_num; ++i)
    {
        auto pk = fmt::format("mock_rowkey_{:06d}", i);
        auto key = TiKVKey::copyFrom(pk);
        auto val = TiKVValue::copyFrom(RecordKVFormat::encodeLockCfValue(Region::PutFlag, pk, /*ts=*/1000, 0));
        if (i % 1'000'000 == 0)
            LOG_INFO(log, "rows write done, i={}", i);
        reg_data.insert(ColumnFamilyType::Lock, std::move(key), std::move(val));
    }

    WriteBufferFromOwnString buff;
    auto serialized_size = reg_data.serialize(buff);

    LOG_INFO(log, "data_size={} serialized_size={}", reg_data.dataSize(), serialized_size);

    process_mem_usage(real_rss, proc_thrds, vitr_size);
    Int64 real_rss_after_insert = real_rss;

    LOG_INFO(
        log,
        "real_rss_0={} real_rss_1={} diff={}",
        formatReadableSizeWithBinarySuffix(real_rss_before_insert),
        formatReadableSizeWithBinarySuffix(real_rss_after_insert),
        formatReadableSizeWithBinarySuffix(real_rss_after_insert - real_rss_before_insert));

    // Remove all keys
    for (size_t i = 0; i < test_num; ++i)
    {
        auto pk = fmt::format("mock_rowkey_{:06d}", i);
        auto key = TiKVKey::copyFrom(pk);
        reg_data.remove(ColumnFamilyType::Lock, key);
    }
    process_mem_usage(real_rss, proc_thrds, vitr_size);
    Int64 real_rss_after_del = real_rss;

    LOG_INFO(
        log,
        "real_rss_0={} real_rss_1={} real_rss_2={} diff={}",
        formatReadableSizeWithBinarySuffix(real_rss_before_insert),
        formatReadableSizeWithBinarySuffix(real_rss_after_insert),
        formatReadableSizeWithBinarySuffix(real_rss_after_del),
        formatReadableSizeWithBinarySuffix(real_rss_after_insert - real_rss_after_del));
}
#endif

} // namespace DB::tests
