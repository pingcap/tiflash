#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Server/DMTool/DMTool.h>
#include <Storages/DeltaMerge/File/DMFile.h>

#include <boost/program_options.hpp>
#include <iostream>
namespace bpo = boost::program_options;

namespace DTTool::Inspect
{
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

int inspectServiceMain(DB::Context & context, const InspectArgs & args)
{
    auto black_hole = reinterpret_cast<char *>(::operator new (DBMS_DEFAULT_BUFFER_SIZE, std::align_val_t{64}));
    SCOPE_EXIT({ ::operator delete (black_hole, std::align_val_t{64}); });
    auto consume = [&](DB::ReadBuffer & t) {
        while (t.readBig(black_hole, DBMS_DEFAULT_BUFFER_SIZE) != 0) {}
    };
    auto fp = context.getFileProvider();
    auto dmfile = DB::DM::DMFile::restore(fp, args.file_id, 0, args.workdir, DB::DM::DMFile::ReadMetaMode::all());
    std::cout << "bytes on disk: " << dmfile->getBytesOnDisk() << std::endl;
    std::cout << "single file: " << dmfile->isSingleFileMode() << std::endl;
    if (auto conf = dmfile->getConfiguration())
    {
        std::cout << "with new checksum: true" << std::endl;
        switch (conf->getChecksumAlgorithm())
        {
        case DB::ChecksumAlgo::None:
            std::cout << "checksum algorithm: none" << std::endl;
            break;
        case DB::ChecksumAlgo::CRC32:
            std::cout << "checksum algorithm: crc32" << std::endl;
            break;
        case DB::ChecksumAlgo::CRC64:
            std::cout << "checksum algorithm: crc64" << std::endl;
            break;
        case DB::ChecksumAlgo::City128:
            std::cout << "checksum algorithm: city128" << std::endl;
            break;
        case DB::ChecksumAlgo::XXH3:
            std::cout << "checksum algorithm: xxh3" << std::endl;
            break;
        }
        for (const auto & [name, msg] : conf->getDebugInfo())
        {
            std::cout << name << ": " << msg << std::endl;
        }
    }
    if (args.check && dmfile->isSingleFileMode())
    {
        std::cerr << "single file integrity checking is not yet supported" << std::endl;
        return -EINVAL;
    }
    if (args.check)
    {
        auto prefix = args.workdir + "/dmf_" + DB::toString(args.file_id);
        auto file = Poco::File{prefix};
        std::vector<std::string> sub;
        file.list(sub);
        for (auto & i : sub)
        {
            if (endsWith(i, ".mrk") || endsWith(i, ".dat") || endsWith(i, ".idx") || i == "pack")
            {
                auto full_path = prefix;
                full_path += "/";
                full_path += i;
                std::cout << "checking " << i << ": ";
                std::cout.flush();
                if (dmfile->getConfiguration())
                {
                    consume(*DB::createReadBufferFromFileBaseByFileProvider(
                        fp,
                        full_path,
                        DB::EncryptionPath(full_path, i),
                        dmfile->getConfiguration()->getChecksumFrameLength(),
                        nullptr,
                        dmfile->getConfiguration()->getChecksumAlgorithm(),
                        dmfile->getConfiguration()->getChecksumFrameLength()));
                }
                else
                {
                    consume(*DB::createReadBufferFromFileBaseByFileProvider(
                        fp,
                        full_path,
                        DB::EncryptionPath(full_path, i),
                        DBMS_DEFAULT_BUFFER_SIZE,
                        0,
                        nullptr));
                }
                std::cout << "[success]" << std::endl;
            }
        }
    }
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
        std::cerr << exception.what() << std::endl
                  << INSPECT_HELP << std::endl;
        return -EINVAL;
    }

    return 0;
}
} // namespace DTTool::Inspect