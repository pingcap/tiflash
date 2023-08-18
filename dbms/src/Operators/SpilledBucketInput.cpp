// Copyright 2023 PingCAP, Inc.
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

#include <Operators/SpilledBucketInput.h>

namespace DB
{
SpilledBucketInput::SpilledBucketInput(const BlockInputStreamPtr & stream_)
    : stream(stream_)
{
    stream->readPrefix();
}

bool SpilledBucketInput::needLoad() const
{
    return !is_exhausted && !output.has_value();
}

void SpilledBucketInput::load()
{
    RUNTIME_CHECK(needLoad());
    Block ret = stream->read();
    if unlikely (!ret)
    {
        is_exhausted = true;
        stream->readSuffix();
    }
    else
    {
        /// Only two level data can be spilled.
        RUNTIME_CHECK(ret.info.bucket_num != -1);
        output.emplace(std::move(ret));
    }
}

bool SpilledBucketInput::hasOutput() const
{
    return output.has_value();
}

Int32 SpilledBucketInput::bucketNum() const
{
    RUNTIME_CHECK(hasOutput());
    return output->info.bucket_num;
}

Block SpilledBucketInput::popOutput()
{
    RUNTIME_CHECK(hasOutput());
    Block ret = std::move(*output);
    output.reset();
    return ret;
}

Int32 SpilledBucketInput::getMinBucketNum(const SpilledBucketInputs & inputs)
{
    RUNTIME_CHECK(!inputs.empty());
    Int32 min_bucket_num = NUM_BUCKETS;
    for (const auto & input : inputs)
    {
        if (input.hasOutput())
            min_bucket_num = std::min(input.bucketNum(), min_bucket_num);
    }
    return min_bucket_num;
}

BlocksList SpilledBucketInput::popOutputs(SpilledBucketInputs & inputs, Int32 target_bucket_num)
{
    BlocksList bucket_data;
    // store bucket data of min bucket num.
    for (auto & input : inputs)
    {
        if (input.hasOutput() && target_bucket_num == input.bucketNum())
            bucket_data.push_back(input.popOutput());
    }
    RUNTIME_CHECK(!bucket_data.empty());
    return bucket_data;
}
} // namespace DB
