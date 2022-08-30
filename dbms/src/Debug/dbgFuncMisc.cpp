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

#include <Common/typeid_cast.h>
#include <Debug/dbgFuncMisc.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/StoragePool.h>

#include <fstream>
#include <regex>

namespace DB
{
inline size_t getReadTSOForLog(const String & line)
{
    try
    {
        std::regex rx(R"((0|[1-9][0-9]*))");
        std::smatch m;
        auto pos = line.find("read_tso=");
        if (pos != std::string::npos && regex_search(line.cbegin() + pos, line.cend(), m, rx))
        {
            return std::stoul(m[1]);
        }
        else
        {
            return 0;
        }
    }
    catch (std::exception & e)
    {
        throw Exception(fmt::format("Parse 'read tso' failed, exception: {}, line {}", e.what(), line));
    }
}

// Usage example:
// The first argument is the key you want to search.
// For example, we want to search the key 'RSFilter exclude rate' in log file, and get the value following it.
// So we can use it as the first argument.
// But many kind of thread can print this keyword,
// so we can use the second argument to specify a keyword that may just be printed by a specific kind of thread.
// Here we use 'Rough set filter' to specify we just want to search read thread.
// And the complete command is the following:
//   DBGInvoke search_log_for_key('RSFilter exclude rate', 'Rough set filter')
// TODO: this is still a too hack way to do test, but cannot think a better way now.
void dbgFuncSearchLogForKey(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 2)
        throw Exception("Args not matched, should be: key, thread_hint", ErrorCodes::BAD_ARGUMENTS);

    auto key = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    // the candidate line must be printed by a thread which also print a line contains `thread_hint`
    auto tso_hint = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    auto log_path = context.getConfigRef().getString("logger.log");

    std::ifstream file(log_path);
    // get the lines containing `thread_hint` and `key`
    std::vector<String> tso_hint_line_candidates;
    std::vector<String> key_line_candidates;
    {
        String line;
        while (std::getline(file, line))
        {
            if ((line.find(tso_hint) != String::npos) && (line.find("DBGInvoke") == String::npos))
                tso_hint_line_candidates.emplace_back(line);
            else if ((line.find(key) != String::npos) && (line.find("DBGInvoke") == String::npos))
                key_line_candidates.emplace_back(line);
        }
    }
    // get target read tso
    if (tso_hint_line_candidates.empty() || key_line_candidates.empty())
    {
        output("Invalid");
        return;
    }
    size_t target_read_tso = getReadTSOForLog(tso_hint_line_candidates.back());
    if (target_read_tso == 0)
    {
        output("Invalid");
        return;
    }
    String target_line;
    for (auto iter = key_line_candidates.rbegin(); iter != key_line_candidates.rend(); iter++)
    {
        if (getReadTSOForLog(*iter) == target_read_tso)
        {
            target_line = *iter;
            break;
        }
    }
    // try parse the first number following the key
    try
    {
        std::regex rx(R"([+-]?([0-9]+([.][0-9]*)?|[.][0-9]+))");
        std::smatch m;
        auto pos = target_line.find(key);
        if (pos != std::string::npos && regex_search(target_line.cbegin() + pos, target_line.cend(), m, rx))
        {
            output(m[1]);
        }
        else
        {
            output("Invalid");
        }
    }
    catch (std::exception & e)
    {
        throw Exception(fmt::format("Parse 'RSFilter exclude rate' failed, exception: {}, target_line {}", e.what(), target_line));
    }
}

void dbgFuncTriggerGlobalPageStorageGC(Context & context, const ASTs & /*args*/, DBGInvoker::Printer /*output*/)
{
    auto global_storage_pool = context.getGlobalStoragePool();
    if (global_storage_pool)
    {
        global_storage_pool->gc();
    }
}
} // namespace DB
