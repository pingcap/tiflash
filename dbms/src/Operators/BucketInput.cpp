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

#include <Operators/BucketInput.h>

namespace DB
{
BucketInput::BucketInput(const BlockInputStreamPtr & stream_)
    : stream(stream_)
{
    stream->readPrefix();
}

bool BucketInput::needLoad() const
{
    return !is_exhausted && !output.has_value();
}

void BucketInput::load()
{
    assert(needLoad());
    Block ret = stream->read();
    if unlikely (!ret)
    {
        is_exhausted = true;
        stream->readSuffix();
    }
    else
    {
        /// Only two level data can be spilled.
        assert(ret.info.bucket_num != -1);
        output.emplace(std::move(ret));
    }
}

bool BucketInput::hasOutput() const
{
    return output.has_value();
}

Int32 BucketInput::bucketNum() const
{
    assert(hasOutput());
    return output->info.bucket_num;
}

Block BucketInput::moveOutput()
{
    assert(hasOutput());
    Block ret = std::move(*output);
    output.reset();
    return ret;
}
} // namespace DB
