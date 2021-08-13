#include <Server/DMTool.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>

#include <boost/program_options.hpp>
#include <iostream>
namespace bpo = boost::program_options;

// clang-format off
static constexpr char INSPECT_HELP[] =
    "Usage: inspect [args]\n"
    "Available Arguments:\n"
    "  --help        Print help message and exit.\n"
    "  --encryption  Target has encryption.\n"
    "  --check       Iterate data files to check integrity.\n"
    "  --workdir     Target directory.";

// clang-format on

int inspectEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge Inspect"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool help_mode = false;
    bool encryption = false;
    bool check = false;

    // clang-format off
    options.add_options()
        ("help", bpo::bool_switch(&help_mode))
        ("check", bpo::bool_switch(&check))
        ("workdir", bpo::value<std::string>()->required())
        ("encryption", bpo::bool_switch(&encryption));
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
            std::cout << INSPECT_HELP << std::endl;
            return 0;
        }
        bpo::notify(vm);
    }
    catch (const boost::wrapexcept<boost::program_options::required_option> & exception)
    {
        std::cerr << exception.what() << std::endl << INSPECT_HELP << std::endl;
        return 1;
    }

    return 0;
}