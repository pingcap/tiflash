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

#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingTransform.h>


namespace DB
{
/** Merging consecutive blocks of stream to specified minimum size.
  */
class SquashingBlockInputStream : public IBlockInputStream
{
    static constexpr auto NAME = "Squashing";

public:
    SquashingBlockInputStream(
        const BlockInputStreamPtr & src,
        size_t min_block_size_rows,
        size_t min_block_size_bytes,
        const String & req_id);

    String getName() const override { return NAME; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

    Block read() override;

private:
    const LoggerPtr log;
    SquashingTransform transform;
    bool all_read = false;
};

} // namespace DB
