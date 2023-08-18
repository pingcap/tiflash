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

#include <DataStreams/SquashingBlockOutputStream.h>


namespace DB
{
SquashingBlockOutputStream::SquashingBlockOutputStream(
    BlockOutputStreamPtr & dst,
    size_t min_block_size_rows,
    size_t min_block_size_bytes)
    : output(dst)
    , transform(min_block_size_rows, min_block_size_bytes, /*req_id=*/"")
{}


void SquashingBlockOutputStream::write(const Block & block)
{
    SquashingTransform::Result result = transform.add(Block(block));
    if (result.ready)
        output->write(result.block);
}


void SquashingBlockOutputStream::finalize()
{
    if (all_written)
        return;

    all_written = true;

    SquashingTransform::Result result = transform.add({});
    if (result.ready && result.block)
        output->write(result.block);
}


void SquashingBlockOutputStream::flush()
{
    if (!disable_flush)
        finalize();
    output->flush();
}


void SquashingBlockOutputStream::writePrefix()
{
    output->writePrefix();
}


void SquashingBlockOutputStream::writeSuffix()
{
    finalize();
    output->writeSuffix();
}

} // namespace DB
