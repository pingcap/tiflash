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

#include <iostream>

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


int main(int, char **)
try
{
    using namespace DB;

    std::string input =
        " SELECT 18446744073709551615, f(1), '\\\\', [a, b, c], (a, b, c), 1 + 2 * -3, a = b OR c > d.1 + 2 * -g[0] AND NOT e < f * (x + y)"
        " FROM default.hits"
        " WHERE CounterID = 101500 AND UniqID % 3 = 0"
        " GROUP BY UniqID"
        " HAVING SUM(Refresh) > 100"
        " ORDER BY Visits, PageViews"
        " LIMIT 1000, 10"
        " INTO OUTFILE 'test.out'"
        " FORMAT TabSeparated";

    ParserQueryWithOutput parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);

    std::cout << "Success." << std::endl;
    formatAST(*ast, std::cerr);
    std::cout << std::endl;

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
