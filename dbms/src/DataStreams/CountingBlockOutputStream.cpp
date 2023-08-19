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

#include <Common/ProfileEvents.h>
#include <DataStreams/CountingBlockOutputStream.h>

namespace DB
{
void CountingBlockOutputStream::write(const Block & block)
{
    stream->write(block);

    Progress local_progress(block.rows(), block.bytes(), 0);
    progress.incrementPiecewiseAtomically(local_progress);

    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);
}

} // namespace DB
