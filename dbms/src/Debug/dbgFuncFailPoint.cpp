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
#include <Common/typeid_cast.h>
#include <Debug/dbgFuncFailPoint.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{
void DbgFailPointFunc::dbgInitFailPoint(Context &, const ASTs &, DBGInvoker::Printer)
{
    (void)fiu_init(0);
}

void DbgFailPointFunc::dbgEnablePauseFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: name", ErrorCodes::BAD_ARGUMENTS);
    const String fail_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    auto time = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    FailPointHelper::enablePauseFailPoint(fail_name, time);
}

void DbgFailPointFunc::dbgEnableFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: name", ErrorCodes::BAD_ARGUMENTS);

    const String fail_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    FailPointHelper::enableFailPoint(fail_name);
}

void DbgFailPointFunc::dbgDisableFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: name", ErrorCodes::BAD_ARGUMENTS);

    String fail_name;
    if (const auto * ast = typeid_cast<const ASTAsterisk *>(args[0].get()); ast != nullptr)
        fail_name = "*";
    else if (const auto * ast = typeid_cast<const ASTIdentifier *>(args[0].get()); ast != nullptr)
        fail_name = ast->name;
    else
        throw Exception("Can not parse arg[0], should be: name or *", ErrorCodes::BAD_ARGUMENTS);

    FailPointHelper::disableFailPoint(fail_name);
}

void DbgFailPointFunc::dbgWaitFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: name", ErrorCodes::BAD_ARGUMENTS);

    const String fail_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    // Wait for the failpoint till it is disabled.
    // Return if it is already disabled.
    FAIL_POINT_PAUSE(fail_name.c_str()); // NOLINT
}

} // namespace DB
