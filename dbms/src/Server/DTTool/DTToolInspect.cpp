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

#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <common/logger_useful.h>

#include <boost/program_options.hpp>
#include <iostream>
#include <random>
namespace bpo = boost::program_options;

namespace DTTool::Inspect
{
// clang-format off
static constexpr char INSPECT_HELP[] =
    "Usage: inspect [args]\n"
    "Available Arguments:\n"
    "  --help        Print help message and exit.\n"
    "  --config-file TiFlash config file.\n"
    "  --check       Iterate data files to check integrity.\n"
    "  --file-id     Target DTFile ID.\n"
    "  --imitative   Use imitative context instead. (encryption is not supported in this mode)\n"
    "  --workdir     Target directory.";

// clang-format on

int inspectServiceMain(DB::Context & context, const InspectArgs & args)
{
    // from this part, the base daemon is running, so we use logger instead
    auto * logger = &Poco::Logger::get("DTToolInspect");

    // black_hole is used to consume data manually.
    // we use SCOPE_EXIT to ensure the release of memory area.
    auto * black_hole = reinterpret_cast<char *>(::operator new (DBMS_DEFAULT_BUFFER_SIZE, std::align_val_t{64}));
    SCOPE_EXIT({ ::operator delete (black_hole, std::align_val_t{64}); });
    auto consume = [&](DB::ReadBuffer & t) {
        while (t.readBig(black_hole, DBMS_DEFAULT_BUFFER_SIZE) != 0) {}
    };

    // Open the DMFile at `workdir/dmf_<file-id>`
    auto fp = context.getFileProvider();
    auto dmfile = DB::DM::DMFile::restore(fp, args.file_id, 0, args.workdir, DB::DM::DMFile::ReadMetaMode::all());

    LOG_FMT_INFO(logger, "bytes on disk: {}", dmfile->getBytesOnDisk());
    LOG_FMT_INFO(logger, "single file: {}", dmfile->isSingleFileMode());

    // if the DMFile has a config file, there may be additional debugging information
    // we also log the content of dmfile checksum config
    if (auto conf = dmfile->getConfiguration())
    {
        LOG_FMT_INFO(logger, "with new checksum: true");
        switch (conf->getChecksumAlgorithm())
        {
        case DB::ChecksumAlgo::None:
            LOG_FMT_INFO(logger, "checksum algorithm: none");
            break;
        case DB::ChecksumAlgo::CRC32:
            LOG_FMT_INFO(logger, "checksum algorithm: crc32");
            break;
        case DB::ChecksumAlgo::CRC64:
            LOG_FMT_INFO(logger, "checksum algorithm: crc64");
            break;
        case DB::ChecksumAlgo::City128:
            LOG_FMT_INFO(logger, "checksum algorithm: city128");
            break;
        case DB::ChecksumAlgo::XXH3:
            LOG_FMT_INFO(logger, "checksum algorithm: xxh3");
            break;
        }
        for (const auto & [name, msg] : conf->getDebugInfo())
        {
            LOG_FMT_INFO(logger, "{}: {}", name, msg);
        }
    }

    if (args.check)
    {
        // for directory mode file, we can consume each file to check its integrity.
        if (!dmfile->isSingleFileMode())
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
                    LOG_FMT_INFO(logger, "checking {}: ", i);
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
                    LOG_FMT_INFO(logger, "[success]");
                }
            }
        }
        // for both directory file and single mode file, we can read out all blocks from the file.
        // this procedure will also trigger the checksum checking in the compression buffer.
        LOG_FMT_INFO(logger, "examine all data blocks: ");
        {
            auto stream = DB::DM::createSimpleBlockInputStream(context, dmfile);
            size_t counter = 0;
            stream->readPrefix();
            while (stream->read())
            {
                counter++;
            }
            stream->readSuffix();
            LOG_FMT_INFO(logger, "[success] ( {} blocks )", counter);
        }
    }
    return 0;
}


int inspectEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function)
{
    bpo::options_description options{"Delta Merge Inspect"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool check = false;
    bool imitative = false;
    // clang-format off
    options.add_options()
        ("help", "")
        ("check", bpo::bool_switch(&check))
        ("workdir", bpo::value<std::string>()->required())
        ("file-id", bpo::value<size_t>()->required())
        ("imitative", bpo::bool_switch(&imitative))
        ("config-file", bpo::value<std::string>());
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

        if (imitative && vm.count("config-file") != 0)
        {
            std::cerr << "config-file is not allowed in imitative mode" << std::endl;
            return -EINVAL;
        }
        else if (!imitative && vm.count("config-file") == 0)
        {
            std::cerr << "config-file is required in proxy mode" << std::endl;
            return -EINVAL;
        }

        auto workdir = vm["workdir"].as<std::string>();
        auto file_id = vm["file-id"].as<size_t>();
        auto args = InspectArgs{check, file_id, workdir};

        if (imitative)
        {
            auto env = detail::ImitativeEnv{args.workdir};
            return inspectServiceMain(*env.getContext(), args);
        }
        else
        {
            auto config_file = vm["config-file"].as<std::string>();
            CLIService service(inspectServiceMain, args, config_file, ffi_function);
            return service.run({""});
        }
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
