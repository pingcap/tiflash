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

#include <Storages/Page/tools/PageCtl/PageStorageCtl.h>
#include <Storages/Page/tools/PageCtl/PageStorageCtlV2.h>
#include <Storages/Page/tools/PageCtl/PageStorageCtlV3.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wdeprecated-declarations"

#include <boost_wrapper/program_options.h>

#pragma GCC diagnostic pop

#include <iostream>
namespace DB
{
int PageStorageCtl::mainEntry(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using po::value;
    po::options_description desc("Allowed commands");
    desc.add_options()("help,h", "produce help message") //
        ("page_storage_version,V",
         value<int>(),
         "PageStorage Version: 2 means PageStorage V2, 3 means PageStorage V3, 4 means UniversalPageStorage.\n");
    po::variables_map options;
    po::parsed_options parsed = po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
    po::store(parsed, options);
    po::notify(options);
    if (options.count("page_storage_version") == 0 && options.count("help") == 0)
    {
        std::cerr << "Invalid arg page_storage_version." << std::endl;
        std::cerr << desc << std::endl;
        exit(0);
    }
    if (options.count("page_storage_version") > 0)
    {
        int ps_version = options["page_storage_version"].as<int>();
        if (ps_version == 4)
        {
            universalpageStorageCtlEntry(argc - 2, argv + 2);
            return 0;
        }
        else if (ps_version == 3)
        {
            pageStorageV3CtlEntry(argc - 2, argv + 2);
            return 0;
        }
        else if (ps_version == 2)
        {
            return pageStorageV2CtlEntry(argc - 2, argv + 2);
        }
        else
        {
            std::cerr << "Invalid arg page_storage_version." << std::endl;
            std::cerr << desc << std::endl;
            exit(0);
        }
    }
    if (options.count("help") > 0)
    {
        std::cerr << desc << std::endl;
        exit(0);
    }
    return 0;
}
} // namespace DB
