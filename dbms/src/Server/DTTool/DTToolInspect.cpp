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
#include <Common/FieldVisitors.h>
#include <Common/FmtUtils.h>
#include <Common/MyTime.h>
#include <Common/formatReadable.h>
#include <Core/Types.h>
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>
#include <boost_wrapper/program_options.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <iostream>

namespace bpo = boost::program_options;

namespace DTTool::Inspect
{

DB::DM::ColumnDefines getColumnsToDump(
    const DB::DM::DMFilePtr & dmfile,
    const std::vector<DB::ColumnID> & col_ids,
    bool dump_all_columns)
{
    const auto & all_columns = dmfile->getColumnDefines(/*sort_by_id=*/true);
    if (dump_all_columns)
        return all_columns;

    DB::DM::ColumnDefines cols_to_dump;
    for (const auto & c : all_columns)
    {
        // Dump the extra-handle, version and delmark columns
        // by default
        if (c.id == DB::MutSup::extra_handle_id //
            || c.id == DB::MutSup::version_col_id //
            || c.id == DB::MutSup::delmark_col_id)
            cols_to_dump.emplace_back(c);

        if (!col_ids.empty())
        {
            // If specific column IDs are provided, also dump those columns
            if (std::find(col_ids.begin(), col_ids.end(), c.id) != col_ids.end())
                cols_to_dump.emplace_back(c);
        }
    }
    return cols_to_dump;
}

String getMinMaxCellAsString(const DB::DM::MinMaxIndex::Cell & cell, const DB::DataTypePtr & dtype)
{
    if (!cell.has_value)
        return "value=(no value)";

    String res = fmt::format( //
        "min={} max={}",
        DB::applyVisitor(DB::FieldVisitorDump(), cell.min),
        DB::applyVisitor(DB::FieldVisitorDump(), cell.max));

    if (dtype->getTypeId() == DB::TypeIndex::MyDateTime || dtype->getTypeId() == DB::TypeIndex::MyDate
        || dtype->getTypeId() == DB::TypeIndex::MyTime || dtype->getTypeId() == DB::TypeIndex::MyTimeStamp)
    {
        DB::MyDateTime min_tm(cell.min.get<UInt64>());
        DB::MyDateTime max_tm(cell.max.get<UInt64>());
        res += fmt::format( //
            " min_as_time={} max_as_time={}",
            min_tm.toString(0),
            max_tm.toString(0));
    }
    return res;
}


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

    {
        const auto all_cols = dmfile->getColumnDefines();
        LOG_INFO(logger, "Dumping column defines, num_columns={}", all_cols.size());
        for (const auto & col : all_cols)
        {
            LOG_INFO(logger, "col_id={} col_name={} col_type={}", col.id, col.name, col.type->getName());
        }
    }
    {
        const auto & pack_stats = dmfile->getPackStats();
        const auto & pack_prop = dmfile->getPackProperties();
        LOG_INFO(
            logger,
            "Dumping pack stats, num_packs={} num_properties={}",
            pack_stats.size(),
            pack_prop.property_size());
        for (size_t i = 0; i < pack_stats.size(); ++i)
        {
            const auto & pack_stat = pack_stats[i];
            String prop_str = "(no property)";
            if (pack_prop.property_size() > static_cast<Int64>(i))
            {
                const auto & prop = pack_prop.property(i);
                prop_str = fmt::format("{}", prop.ShortDebugString());
            }
            LOG_INFO(logger, "pack_id={} pack_stat={} prop={}", i, pack_stat.toDebugString(), prop_str);
        }
    }

    if (args.dump_merged_files)
    {
        if (!dmfile->useMetaV2())
        {
            LOG_INFO(logger, "Merged files are not supported in this DMFile version.");
        }
        else
        {
            auto * dmfile_meta = typeid_cast<DB::DM::DMFileMetaV2 *>(dmfile->getMeta().get());
            assert(dmfile_meta != nullptr);
            LOG_INFO(logger, "Dumping merged files: ");
            for (const auto & [_, sub_file] : dmfile_meta->merged_sub_file_infos)
            {
                LOG_INFO(
                    logger,
                    "filename={} merged_file_id={} offset={} size={}",
                    sub_file.fname,
                    sub_file.number,
                    sub_file.offset,
                    sub_file.size);
            }
            LOG_INFO(logger, "total merged sub files num={}", dmfile_meta->merged_sub_file_infos.size());

            for (const auto & merged_file : dmfile_meta->merged_files)
            {
                LOG_INFO(logger, "merged_file_id={} size={}", merged_file.number, merged_file.size);
            }
            LOG_INFO(logger, "total merged files num={}", dmfile_meta->merged_files.size());
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

    if (args.dump_minmax)
    {
        LOG_INFO(logger, "dumping minmax values from all data blocks");
        const DB::DM::ColumnDefines cols_to_dump = getColumnsToDump(dmfile, args.col_ids, args.dump_all_columns);
        for (const auto & col : cols_to_dump)
        {
            LOG_INFO(
                logger,
                "dump minmax for column: column_id={} name={} type={}",
                col.id,
                col.name,
                col.type->getName());
        }
        for (const auto & c : cols_to_dump)
        {
            const Int64 col_id = c.id;
            if (!args.col_ids.empty())
            {
                // If specific column IDs are provided, only dump those columns
                if (std::find(args.col_ids.begin(), args.col_ids.end(), col_id) == args.col_ids.end())
                    continue;
            }

            DB::DataTypePtr dtype;
            DB::DM::MinMaxIndexPtr minmax_idx;
            try
            {
                std::tie(dtype, minmax_idx) = DB::DM::DMFilePackFilter::loadIndex( //
                    *dmfile,
                    fp,
                    nullptr,
                    /*block_wait=*/false,
                    /*set_cache_if_miss=*/false,
                    col_id,
                    nullptr,
                    nullptr);
            }
            catch (const DB::Exception & e)
            {
                // just ignore
            }

            if (minmax_idx == nullptr)
            {
                LOG_INFO(logger, "minmax index, col_id={} type={} null", col_id, c.type->getName());
                continue;
            }
            for (size_t pack_no = 0; pack_no < minmax_idx->size(); ++pack_no)
            {
                auto cell = minmax_idx->getCell(pack_no);
                LOG_INFO(
                    logger,
                    "minmax index, col_id={} type={} pack_no={} {}",
                    col_id,
                    dtype->getName(),
                    pack_no,
                    getMinMaxCellAsString(cell, dtype));
            }
        }
    } // end of (arg.dump_minmax)

    if (args.dump_columns || args.dump_all_columns)
    {
        LOG_INFO(logger, "dumping values from all data blocks");
        const DB::DM::ColumnDefines cols_to_dump = getColumnsToDump(dmfile, args.col_ids, args.dump_all_columns);
        for (const auto & col : cols_to_dump)
        {
            LOG_INFO(
                logger,
                "dump value for column: column_id={} name={} type={}",
                col.id,
                col.name,
                col.type->getName());
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
        for (const auto & cd : cols_to_dump)
        {
            auto column_id = cd.id;
            auto col_in_mem_bytes = in_mem_bytes[column_id];
            LOG_INFO(
                logger,
                "column_id={} bytes_in_mem={}",
                column_id,
                formatReadableSizeWithBinarySuffix(col_in_mem_bytes));
        }
    } // end of (arg.dump_columns)
    return 0;
}

std::optional<std::vector<DB::ColumnID>> parseColumnIDs(const DB::String & col_ids_str)
{
    std::vector<DB::ColumnID> col_ids;
    if (col_ids_str.empty())
        return col_ids;
    std::vector<String> col_ids_vec;
    boost::split(col_ids_vec, col_ids_str, boost::is_any_of(","));
    col_ids.reserve(col_ids_vec.size());
    for (const auto & cid_str : col_ids_vec)
    {
        try
        {
            col_ids.push_back(DB::parse<DB::ColumnID>(cid_str));
        }
        catch (const DB::Exception & e)
        {
            return std::nullopt; // Return empty optional if any column ID is invalid
        }
    }
    return col_ids;
}


int inspectEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function)
{
    bool check = false;
    bool imitative = false;
    bool dump_columns = false;
    bool dump_all_columns = false;
    bool dump_minmax = false;
    bool dump_merged_files = false;

    bpo::variables_map vm;
    bpo::options_description options{"Delta Merge Inspect"};
    options.add_options() //
        ("help", "Print help message and exit.") //
        ("check", bpo::bool_switch(&check), "Check integrity for the delta-tree file.") //
        ("dump", bpo::bool_switch(&dump_columns), "Dump the handle, pk, tag column values.") //
        ("dump-all", bpo::bool_switch(&dump_all_columns), "Dump all column values.") //
        ("dump-merged-files",
         bpo::bool_switch(&dump_merged_files),
         "Dump the merged files in the delta-tree file.") //
        ("minmax", bpo::bool_switch(&dump_minmax), "Dump the minmax values") //
        ("col-ids",
         bpo::value<std::string>()->default_value(""),
         "Dump the specified column IDs, separated by comma. "
         "If this option is specified, only the specified columns will be dumped. "
         "This option is only valid when --dump or --minmax is specified.") //>)
        ("workdir",
         bpo::value<std::string>()->required(),
         "Target directory. Will inspect the delta-tree file ${workdir}/dmf_${file-id}/") //
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
        auto col_ids_str = vm["col-ids"].as<std::string>();
        std::optional<std::vector<DB::ColumnID>> col_ids = parseColumnIDs(col_ids_str);
        if (!col_ids)
        {
            std::cerr << "Invalid column IDs: " << col_ids_str << std::endl;
            return -EINVAL;
        }

        auto args = InspectArgs{
            check,
            dump_columns,
            dump_all_columns,
            dump_minmax,
            dump_merged_files,
            file_id,
            workdir,
            col_ids.value(),
        };
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
