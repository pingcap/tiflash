#include <Server/DMTool.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>

#include <boost/program_options.hpp>
#include <iostream>
namespace bpo = boost::program_options;

// clang-format off
static constexpr char MIGRATE_HELP[] =
    "Usage: migrate [args]\n"
    "Available Arguments:\n"
    "  --help        Print help message and exit.\n"
    "  --version     Target dmfile version. [default: 2] [available: 1, 2]\n"
    "  --algorithm   Checksum algorithm. [default: xxh3] [available: xxh3, city128, crc32, crc64, none]\n"
    "  --frame       Checksum frame length. [default: " TO_STRING(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE) "]\n"
    "  --workdir     Target directory.\n"
    "  --dry         Dry run: only print change list.";

// clang-format on

int migrateEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge Migration"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool help_mode = false;
    bool dry_mode = false;

    // clang-format off
    options.add_options()
        ("help", bpo::bool_switch(&help_mode))
        ("version", bpo::value<size_t>()->default_value(2))
        ("algorithm", bpo::value<std::string>()->default_value("xxh3"))
        ("frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE))
        ("workdir", bpo::value<std::string>()->required())
        ("dry", bpo::bool_switch(&dry_mode));
    // clang-format on

    bpo::store(bpo::command_line_parser(opts)
                   .options(options)
                   .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                   .run(),
        vm);

    try
    {
        if (vm.count("help"))
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