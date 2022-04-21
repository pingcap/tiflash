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

#include <TestUtils/InterpreterTestUtils.h>
#include <Common/FmtUtils.h>
#include <TestUtils/executorSerializer.h>

namespace DB::tests
{
namespace
{
// String toTreeString(const tipb::Executor & root_executor, size_t level = 0);

// // serialize tipb::DAGRequest, print the executor name in a Tree format.
// String toTreeString(std::shared_ptr<tipb::DAGRequest> dag_request)
// {
//     assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
//     if (dag_request->has_root_executor())
//     {
//         return toTreeString(dag_request->root_executor());
//     }
//     else
//     {
//         FmtBuffer buffer;
//         String prefix;
//         traverseExecutors(dag_request.get(), [&buffer, &prefix](const tipb::Executor & executor) {
//             assert(executor.has_executor_id());
//             buffer.fmtAppend("{}{}\n", prefix, executor.executor_id());
//             prefix.append(" ");
//             return true;
//         });
//         return buffer.toString();
//     }
// }

// String toTreeString(const tipb::Executor & root_executor, size_t level)
// {
//     FmtBuffer buffer;

    // auto append_str = [&buffer, &level](const tipb::Executor & executor) {
    //     assert(executor.has_executor_id());

    //     buffer.append(String(level, ' '));
    //     buffer.append(executor.executor_id()).append("\n");
    // };

//     traverseExecutorTree(root_executor, [&](const tipb::Executor & executor) {
//         if (executor.has_join())
//         {
//             append_str(executor);
//             ++level;
//             for (const auto & child : executor.join().children())
//                 buffer.append(toTreeString(child, level));
//             return false;
//         }
//         else
//         {
//             append_str(executor);
//             ++level;
//             return true;
//         }
//     });

//     return buffer.toString();
// }
} // namespace

} // namespace DB::tests
