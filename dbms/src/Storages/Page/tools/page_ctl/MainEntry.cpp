//
// Created by tongli chen on 2022/5/25.
//

#include <Storages/Page/tools/page_ctl/PageStorageCtl.h>
#include <Storages/Page/tools/page_ctl/page_storage_ctl_v2.h>
#include <Storages/Page/tools/page_ctl/page_storage_ctl_v3.h>

#include <boost/program_options.hpp>
#include <iostream>
namespace DB
{
int PageStorageCtl::mainEntry(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using po::value;
    po::options_description desc("Allowed commands");
    desc.add_options()("help,h", "produce help message") //
        ("page_storage_version,V", value<int>(), "PageStorage Version: 2 means PageStorage V2, 3 means PageStorage V3.\n");
    po::variables_map options;
    po::parsed_options parsed = po::command_line_parser(argc, argv)
                                    .options(desc)
                                    .allow_unregistered()
                                    .run();
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
        int ps_mode = options["page_storage_version"].as<int>();
        if (ps_mode == 3)
        {
            pageStorageV3CtlEntry(argc - 2, argv + 2);
            return 0;
        }
        else if (ps_mode == 2)
        {
            return pageStorageV2CtlEntry(argc - 1, argv + 1);
        }
        else
        {
            std::cerr << "Invalid arg page_storage_mode." << std::endl;
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
