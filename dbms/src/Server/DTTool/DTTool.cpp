#include <Server/DTTool/DTTool.h>

#include <boost/program_options.hpp>
#include <iostream>

namespace bpo = boost::program_options;

namespace DTTool
{
// clang-format off
static constexpr char MAIN_HELP[] =
    "Usage: dttool <subcommand> [args]\n"
    "Available Subcommands:\n"
    "  help        Print help message and exit.\n"
    "  migrate     Migrate dtfile version.\n"
    "  inspect     Inspect dtfile info.\n"
    "  bench       Benchmark dtfile IO performance.";
// clang-format on

extern "C" {
void run_raftstore_proxy_ffi(int argc, const char * const * argv, const DB::EngineStoreServerHelper *);
}

int mainEntryTiFlashDTTool(int argc, char ** argv)
{
    bpo::options_description options{"Delta Merge Tools"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;

    // clang-format off
    options.add_options()
        ("command", bpo::value<std::string>())
        ("args",    bpo::value<std::vector<std::string>>());

    positional
        .add("command",  1)
        .add("args",    -1);

    bpo::parsed_options parsed = bpo::command_line_parser(argc, argv)
        .options(options)
        .positional(positional)
        .allow_unregistered()
        .run();
    // clang-format on
    bpo::store(parsed, vm);
    bpo::notify(vm);
    try
    {
        auto command = vm["command"].as<std::string>();
        if (command == "help")
        {
            std::cout << MAIN_HELP << std::endl;
        }
        else if (command == "bench")
        {
            std::vector<std::string> opts = bpo::collect_unrecognized(parsed.options, bpo::include_positional);
            opts.erase(opts.begin());
            return Bench::benchEntry(opts);
        }
        else if (command == "migrate")
        {
            std::vector<std::string> opts = bpo::collect_unrecognized(parsed.options, bpo::include_positional);
            opts.erase(opts.begin());
            return Migrate::migrateEntry(opts, run_raftstore_proxy_ffi);
        }
        else if (command == "inspect")
        {
            std::vector<std::string> opts = bpo::collect_unrecognized(parsed.options, bpo::include_positional);
            opts.erase(opts.begin());
            return Inspect::inspectEntry(opts, run_raftstore_proxy_ffi);
        }
        else
        {
            std::cerr << "unrecognized subcommand, type `help` to see the help message" << std::endl;
            return -EINVAL;
        }
    }
    catch (const boost::wrapexcept<boost::bad_any_cast> &)
    {
        std::cerr << MAIN_HELP << std::endl;
        return -EINVAL;
    }

    return 0;
}
} // namespace DTTool
