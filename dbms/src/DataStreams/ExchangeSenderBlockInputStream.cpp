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

#include <Common/FailPoint.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>

namespace DB
{
namespace FailPoints
{
extern const char hang_in_execution[];
extern const char exception_during_mpp_non_root_task_run[];
extern const char exception_during_mpp_root_task_run[];
} // namespace FailPoints

Block ExchangeSenderBlockInputStream::readImpl()
{
    FAIL_POINT_PAUSE(FailPoints::hang_in_execution);
    if (writer->dagContext().isRootMPPTask())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_root_task_run);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_non_root_task_run);
    }

    Block block = children.back()->read();
    if (block)
    {
        total_rows += block.rows();
        writer->write(block);
    }
    else
    {
        writer->flush();
    }
    return block;
}
} // namespace DB
