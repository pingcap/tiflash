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

#include <Debug/DBGInvoker.h>

namespace DB
{
struct DbgFailPointFunc
{
    // Init fail point. must be called if you want to enable / disable failpoints
    //    DBGInvoke init_fail_point()
    static void dbgInitFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Enable fail point.
    //    DBGInvoke enable_fail_point(name)
    static void dbgEnableFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Enable pause fail point
    //    DBGInvoke enable_pause_fail_point(name, time)
    //    time == 0 mean pause until the fail point is disabled
    static void dbgEnablePauseFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Disable fail point.
    // Usage:
    //   ./stoage-client.sh "DBGInvoke disable_fail_point(name)"
    //     name == "*" means disable all fail point
    static void dbgDisableFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Wait till the fail point is disabled.
    //    DBGInvoke wait_fail_point(name)
    static void dbgWaitFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);
};

} // namespace DB
