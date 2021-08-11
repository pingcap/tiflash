#include <Server/DMTool.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>

#include <boost/program_options.hpp>
#include <iostream>
#define _TO_STRING(X) #X
#define TO_STRING(X) _TO_STRING(X)
namespace bpo = boost::program_options;

// clang-format off
static constexpr char MAIN_HELP[] =
    "Usage: dmtool <subcommand> [args]\n"
    "Available Subcommands:\n"
    "  help        Print help message and exit.\n"
    "  migrate     Migrate dmfile version.\n"
    "  bench       Benchmark dmfile IO performance.";

static constexpr char BENCH_HELP[] =
    "Usage: bench [args]\n"
    "Available Arguments:\n"
    "  -h,--help        Print help message and exit.\n"
    "  -v,--version     DMFile version. [default: 2] [available: 1, 2]\n"
    "  -a,--algorithm   Checksum algorithm. [default: xxh3] [available: xxh3, city128, crc32, crc64, none]\n"
    "  -k,--frame       Checksum frame length. [default: " TO_STRING(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE) "]\n"
    "  -c,--column      Column number. [default: 100]\n"
    "  -s,--size        Column size.   [default: 1000]\n"
    "  -f,--field       Field length limit. [default: 1024]\n"
    "  -r,--random      Random seed. (optional)\n";

static constexpr char MIGRATE_HELP[] =
    "Usage: migrate [args]\n"
    "Available Arguments:\n"
    "  -h,--help        Print help message and exit.\n"
    "  -v,--version     Target dmfile version. [default: 2] [available: 1, 2]\n"
    "  -a,--algorithm   Checksum algorithm. [default: xxh3] [available: xxh3, city128, crc32, crc64, none]\n"
    "  -k,--frame       Checksum frame length. [default: " TO_STRING(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE) "]\n"
    "  -w,--workdir     Target directory.\n"
    "  -d,--dry         Dry run: only print change list.";

// clang-format on

int benchEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge IO Bench"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool help_mode = false;

    // clang-format off
    options.add_options()
        ("h,help", bpo::bool_switch(&help_mode))
        ("v,version", bpo::value<size_t>()->default_value(2))
        ("a,algorithm", bpo::value<std::string>()->default_value("xxh3"))
        ("k,frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE))
        ("c,column", bpo::value<size_t>()->default_value(100))
        ("s,size", bpo::value<size_t>()->default_value(1000))
        ("f,field", bpo::value<size_t>()->default_value(1024))
        ("r,random", bpo::value<size_t>());
    // clang-format on

    bpo::store(bpo::command_line_parser(opts)
                   .options(options)
                   .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                   .run(),
        vm);

    bpo::notify(vm);

    if (help_mode)
    {
        std::cout << BENCH_HELP << std::endl;
        return 0;
    }

    return 0;
}

int migrateEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge IO Bench"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool help_mode = false;
    bool dry_mode = false;

    // clang-format off
    options.add_options()
        ("h,help", bpo::bool_switch(&help_mode))
        ("v,version", bpo::value<size_t>()->default_value(2))
        ("a,algorithm", bpo::value<std::string>()->default_value("xxh3"))
        ("k,frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE))
        ("w,workdir", bpo::value<std::string>()->required())
        ("d,dry", bpo::bool_switch(&dry_mode));
    // clang-format on

    bpo::store(bpo::command_line_parser(opts)
                   .options(options)
                   .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                   .run(),
        vm);

    try
    {
        if (vm.count("h") || vm.count("help"))
        {
            std::cout << MIGRATE_HELP << std::endl;
            return 0;
        }
        bpo::notify(vm);
    }
    catch (const boost::wrapexcept<boost::program_options::required_option> & exception)
    {
        std::cerr << exception.what() << std::endl << MIGRATE_HELP << std::endl;
        return 1;
    }

    return 0;
}

int mainEntryTiFlashDMTool(int argc, char ** argv)
{
    bpo::options_description options{"Delta Merge Tools"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;

    // clang-format off
    options.add_options()
        ("command", bpo::value<std::string>())
        ("args",    "args to subcommand");

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
            return benchEntry(opts);
        }
        else if (command == "migrate")
        {
            std::vector<std::string> opts = bpo::collect_unrecognized(parsed.options, bpo::include_positional);
            opts.erase(opts.begin());
            return migrateEntry(opts);
        }
        else
        {
            std::cerr << "unrecognized subcommand, type `help` to see the help message" << std::endl;
            return 1;
        }
    }
    catch (const boost::wrapexcept<boost::bad_any_cast> &)
    {
        std::cerr << MAIN_HELP << std::endl;
        return 1;
    }

    return 0;
}
