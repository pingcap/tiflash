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

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <ctime>
#include <ext/scope_guard.h>
#include <memory>
#include "Encryption/createWriteBufferFromFileBaseByFileProvider.h"

namespace DB
{
namespace DM
{
namespace tests
{
class DMBloomFilterIndexTest : public DB::base::TiFlashStorageTestBasic
{
public:
    DMBloomFilterIndexTest() = default;

protected:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        context = DMTestEnv::getContext();
        file_provider = db_context->getFileProvider();
        write_limiter = db_context->getWriteLimiter();
        read_limiter = db_context->getReadLimiter();
        
    }

private:
protected:
    // a ptr to context, we can reload context with different settings if need.
    ContextPtr context;
    FileProviderPtr file_provider;
    WriteLimiterPtr write_limiter;
    ReadLimiterPtr read_limiter;
};

TEST_F(DMBloomFilterIndexTest, BloomFilterWriteAndRead)
try
{
    bloom_parameters parameters;
    parameters.projected_element_count = 8192;
    parameters.false_positive_probability = 0.01;
    parameters.compute_optimal_parameters();

    BloomFilter filter(parameters);
    for (int i = 0; i < 8192; i++){
        filter.insert(i);
    }

    auto encryp_file_path = EncryptionPath("bloom-filter-encryp", ".idx");
    auto buf = createWriteBufferFromFileBaseByFileProvider(file_provider,
                                                    "bloom-filter.idx",
                                                    encryp_file_path,
                                                    false,
                                                    write_limiter,
                                                    DB::ChecksumAlgo::XXH3,
                                                    1048576ULL);
    filter.write(*buf);
    buf->sync();

    auto read_buf = createReadBufferFromFileBaseByFileProvider(file_provider,
                                                    "bloom-filter.idx",
                                                    encryp_file_path,
                                                    1048576ULL,
                                                    read_limiter,
                                                    DB::ChecksumAlgo::XXH3,
                                                    1048576ULL);

    auto read_bloom_filter = BloomFilter::read(*read_buf);
    ASSERT(read_bloom_filter->equal(filter));

    for (int i = 0; i < 8192; i++){
        ASSERT(filter.contains(i));
    }


}CATCH


} // namespace tests
} // namespace DM
} // namespace DB
