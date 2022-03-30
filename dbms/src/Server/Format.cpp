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
#include <boost/program_options.hpp>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>


int mainEntryClickHouseFormat(int argc, char ** argv)
{
    using namespace DB;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("hilite", "add syntax highlight with ANSI terminal escape sequences")
        ("oneline", "format in single line")
        ("quiet,q", "just check syntax, no output on success")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options] < query" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    try
    {
        bool hilite = options.count("hilite");
        bool oneline = options.count("oneline");
        bool quiet = options.count("quiet");

        if (quiet && (hilite || oneline))
        {
            std::cerr << "Options 'hilite' or 'oneline' have no sense in 'quiet' mode." << std::endl;
            return 2;
        }

        String query;
        ReadBufferFromFileDescriptor in(STDIN_FILENO);
        readStringUntilEOF(query, in);

        const char * pos = query.data();
        const char * end = pos + query.size();

        ParserQuery parser(end);
        ASTPtr res = parseQuery(parser, pos, end, "query", 0);

        if (!quiet)
        {
            formatAST(*res, std::cout, hilite, oneline);
            std::cout << std::endl;
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true);
        return getCurrentExceptionCode();
    }

    return 0;
}
