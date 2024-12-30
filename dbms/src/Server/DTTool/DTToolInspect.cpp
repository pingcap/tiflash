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

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/formatReadable.h>
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>

#include <boost/program_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <iostream>

namespace bpo = boost::program_options;

namespace DTTool::Inspect
{
int inspectServiceMain(DB::Context & context, const InspectArgs & args)
{
    // from this part, the base daemon is running, so we use logger instead
    auto logger = DB::Logger::get("DTToolInspect");

    // black_hole is used to consume data manually.
    // we use SCOPE_EXIT to ensure the release of memory area.
    auto * black_hole = reinterpret_cast<char *>(::operator new(DBMS_DEFAULT_BUFFER_SIZE, std::align_val_t{64}));
    SCOPE_EXIT({ ::operator delete(black_hole, std::align_val_t{64}); });
    auto consume = [&](DB::ReadBuffer & t) {
        while (t.readBig(black_hole, DBMS_DEFAULT_BUFFER_SIZE) != 0) {}
    };

    // Open the DMFile at `workdir/dmf_<file-id>`
    auto fp = context.getFileProvider();
    auto dmfile = DB::DM::DMFile::restore(
        fp,
        args.file_id,
        0,
        args.workdir,
        DB::DM::DMFileMeta::ReadMode::all(),
        0 /* FIXME: Support other meta version */);

    LOG_INFO(logger, "bytes on disk: {}", dmfile->getBytesOnDisk());

    // if the DMFile has a config file, there may be additional debugging information
    // we also log the content of dmfile checksum config
    if (auto conf = dmfile->getConfiguration())
    {
        LOG_INFO(logger, "with new checksum: true");
        switch (conf->getChecksumAlgorithm())
        {
        case DB::ChecksumAlgo::None:
            LOG_INFO(logger, "checksum algorithm: none");
            break;
        case DB::ChecksumAlgo::CRC32:
            LOG_INFO(logger, "checksum algorithm: crc32");
            break;
        case DB::ChecksumAlgo::CRC64:
            LOG_INFO(logger, "checksum algorithm: crc64");
            break;
        case DB::ChecksumAlgo::City128:
            LOG_INFO(logger, "checksum algorithm: city128");
            break;
        case DB::ChecksumAlgo::XXH3:
            LOG_INFO(logger, "checksum algorithm: xxh3");
            break;
        }
        for (const auto & [name, msg] : conf->getDebugInfo())
        {
            LOG_INFO(logger, "{}: {}", name, msg);
        }
    }

    if (args.check)
    {
        // for directory mode file, we can consume each file to check its integrity.
        auto prefix = fmt::format("{}/dmf_{}", args.workdir, args.file_id);
        auto file = Poco::File{prefix};
        std::vector<std::string> sub;
        file.list(sub);
        for (auto & i : sub)
        {
            if (endsWith(i, ".mrk") || endsWith(i, ".dat") || endsWith(i, ".idx") || endsWith(i, ".merged")
                || i == "pack" || i == "meta")
            {
                auto full_path = fmt::format("{}/{}", prefix, i);
                LOG_INFO(logger, "checking full_path is {}: ", full_path);
                if (dmfile->getConfiguration())
                {
                    consume(*DB::ChecksumReadBufferBuilder::build(
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
                    consume(*DB::ReadBufferFromRandomAccessFileBuilder::buildPtr(
                        fp,
                        full_path,
                        DB::EncryptionPath(full_path, i)));
                }
                LOG_INFO(logger, "[success]");
            }
        }
        // for both directory file and single mode file, we can read out all blocks from the file.
        // this procedure will also trigger the checksum checking in the compression buffer.
        LOG_INFO(logger, "examine all data blocks: ");
        {
            auto stream = DB::DM::createSimpleBlockInputStream(context, dmfile);
            size_t counter = 0;
            stream->readPrefix();
            while (stream->read())
            {
                counter++;
            }
            stream->readSuffix();
            LOG_INFO(logger, "[success] ( {} blocks )", counter);
        }
    } // end of (arg.check)

    if (args.dump_columns || args.dump_all_columns)
    {
        LOG_INFO(logger, "dumping values from all data blocks");
        // Only dump the extra-handle, version, tag
        const auto all_cols = dmfile->getColumnDefines();
        DB::DM::ColumnDefines cols_to_dump;
        if (args.dump_all_columns)
        {
            cols_to_dump = all_cols;
        }
        else if (args.dump_columns)
        {
            for (const auto & c : all_cols)
            {
                if (c.id == DB::MutSup::extra_handle_id || c.id == DB::MutSup::version_col_id
                    || c.id == DB::MutSup::delmark_col_id)
                    cols_to_dump.emplace_back(c);
            }
        }


        auto stream = DB::DM::createSimpleBlockInputStream(context, dmfile, cols_to_dump);

        size_t tot_num_rows = 0;
        size_t block_no = 0;
        DB::Field f;
        std::map<DB::ColumnID, size_t> in_mem_bytes;

        stream->readPrefix();
        while (true)
        {
            DB::Block block = stream->read();
            if (!block)
                break;

            tot_num_rows += block.rows();
            DB::FmtBuffer buff;
            for (size_t row_no = 0; row_no < block.rows(); ++row_no)
            {
                buff.clear();
                for (size_t col_no = 0; col_no < block.columns(); ++col_no)
                {
                    const auto & col = block.getByPosition(col_no);
                    col.column->get(row_no, f);
                    buff.fmtAppend(
                        "{}{}",
                        DB::applyVisitor(DB::FieldVisitorDump(), f),
                        ((col_no < block.columns() - 1) ? "," : ""));
                }
                LOG_INFO(logger, "pack_no={}, row_no={}, fields=[{}]", block_no, row_no, buff.toString());
            }

            for (const auto & col : block)
            {
                if (auto iter = in_mem_bytes.find(col.column_id); iter != in_mem_bytes.end())
                    iter->second += col.column->byteSize();
                else
                    in_mem_bytes[col.column_id] = col.column->byteSize();
            }
            block_no++;
        }
        stream->readSuffix();

        LOG_INFO(logger, "total_num_rows={}", tot_num_rows);
        for (const auto [column_id, col_in_mem_bytes] : in_mem_bytes)
        {
            LOG_INFO(
                logger,
                "column_id={} bytes_in_mem={}",
                column_id,
                formatReadableSizeWithBinarySuffix(col_in_mem_bytes));
        }
    } // end of (arg.dump_columns)
    return 0;
}


int inspectEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function)
{
    bool check = false;
    bool imitative = false;
    bool dump_columns = false;
    bool dump_all_columns = false;

    bpo::variables_map vm;
    bpo::options_description options{"Delta Merge Inspect"};
    options.add_options() //
        ("help", "Print help message and exit.") //
        ("check", bpo::bool_switch(&check), "Check integrity for the delta-tree file.") //
        ("dump", bpo::bool_switch(&dump_columns), "Dump the handle, pk, tag column values.") //
        ("dump_all", bpo::bool_switch(&dump_all_columns), "Dump all column values.") //
        ("workdir",
         bpo::value<std::string>()->required(),
         "Target directory. Will inpsect the delta-tree file ${workdir}/dmf_${file-id}/") //
        ("file-id", bpo::value<size_t>()->required(), "Target DTFile ID.") //
        ("imitative",
         bpo::bool_switch(&imitative),
         "Use imitative context instead of config file."
         " (encryption is not supported in this mode)") //
        ("config-file", bpo::value<std::string>(), "TiFlash config file.");

    bpo::store(
        bpo::command_line_parser(opts)
            .options(options)
            .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
            .run(),
        vm);

    try
    {
        if (vm.count("help"))
        {
            options.print(std::cerr);
            return 0;
        }
        bpo::notify(vm);

        // either imitative or read from config-file
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
        auto args = InspectArgs{check, dump_columns, dump_all_columns, file_id, workdir};
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
        std::cerr << exception.what() << std::endl;
        options.print(std::cerr);
        return -EINVAL;
    }
    catch (DB::Exception &)
    {
        DB::tryLogCurrentException(DB::Logger::get("DTToolInspect"));
        return -EINVAL;
    }

    return 0;
}
} // namespace DTTool::Inspect
