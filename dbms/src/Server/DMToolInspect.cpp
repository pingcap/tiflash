#include <Server/DMTool.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>
#include <Storages/DeltaMerge/File/DMFile.h>

#include <boost/program_options.hpp>
#include <iostream>
namespace bpo = boost::program_options;

// clang-format off
static constexpr char INSPECT_HELP[] =
    "Usage: inspect [args]\n"
    "Available Arguments:\n"
    "  --help        Print help message and exit.\n"
    "  --config-file Tiflash config file.\n"
    "  --check       Iterate data files to check integrity.\n"
    "  --file-id     Target DMFile ID.\n"
    "  --workdir     Target directory.";

// clang-format on

struct InspectArgs
{
    bool check;
    size_t file_id;
    std::string workdir;
};

int inspectServiceMain(DB::Context & context, const InspectArgs & args)
{
    auto fp = context.getFileProvider();
    auto dmfile = DB::DM::DMFile::restore(fp, args.file_id, 0, args.workdir);
    std::cout << "Bytes On Disk: " << dmfile->getBytesOnDisk() << std::endl;
    return 0;
}


int inspectEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge Inspect"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool check = false;

    // clang-format off
    options.add_options()
        ("help", "")
        ("check", bpo::bool_switch(&check))
        ("workdir", bpo::value<std::string>()->required())
        ("file-id", bpo::value<size_t>()->required())
        ("config-file", bpo::value<std::string>()->required());
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
            std::cerr << INSPECT_HELP << std::endl;
            return 0;
        }
        bpo::notify(vm);
        auto workdir = vm["workdir"].as<std::string>();
        auto file_id = vm["file-id"].as<size_t>();
        auto config_file = vm["config-file"].as<std::string>();
        auto args = InspectArgs{check, file_id, workdir};
        CLIService service(inspectServiceMain, args, config_file);
        return service.run({""});
    }
    catch (const boost::wrapexcept<boost::program_options::required_option> & exception)
    {
        std::cerr << exception.what() << std::endl << INSPECT_HELP << std::endl;
        return 1;
    }

    return 0;
}