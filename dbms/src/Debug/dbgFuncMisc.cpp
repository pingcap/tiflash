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

#include <fstream>
#include <regex>

namespace DB
{
void dbgFuncSearchLogForKey(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 1)
        throw Exception("Args not matched, should be: key", ErrorCodes::BAD_ARGUMENTS);

    String key = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    auto log_path = context.getConfigRef().getString("logger.log");

    std::ifstream file(log_path);
    std::vector<String> line_candidates;
    String line;
    while (std::getline(file, line))
    {
        if ((line.find(key) != String::npos) && (line.find("DBGInvoke") == String::npos))
            line_candidates.emplace_back(line);
    }
    if (line_candidates.empty())
    {
        output("Invalid");
        return;
    }
    auto & target_line = line_candidates.back();
    auto sub_line = target_line.substr(target_line.find(key));
    std::regex rx(R"([+-]?([0-9]+([.][0-9]*)?|[.][0-9]+))");
    std::smatch m;
    if (regex_search(sub_line, m, rx))
        output(m[1]);
    else
        output("Invalid");
}
} // namespace DB
