#include <Encryption/PosixRandomAccessFile.h>
#include <Poco/FileStream.h>
#include <Poco/Path.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/gc/restoreFromCheckpoints.h>
#include <Storages/Page/converter/PageConverter.h>
#include <TestUtils/MockDiskDelegator.h>
#include <fmt/format.h>

#include <filesystem>
#include <fstream>

namespace DB
{
PageConverter::PageConverter(FileProviderPtr file_provider_, PSDiskDelegatorPtr delegator_, PageConverterOptions & options_)
    : delegator(std::move(delegator_))
    , file_provider(file_provider_)
    , options(std::move(options_))
    , log(getLogWithPrefix(nullptr, "PageConverter"))
{
    String paths;
    for (const auto & path : delegator->listPaths())
    {
        paths += path + ", ";
    }
    LOG_FMT_INFO(log, "PageConverter will converter from paths[{}]", paths);
};


char * PageConverter::readV2data(const PageEntry & entry)
{
    // FIXME : we may not need folder_prefix_temp/folder_prefix_legacy
    std::vector<String> folder_prefixs = {
        "page", // folder_prefix_formal
        ".temp.page", // folder_prefix_temp
        "legacy.page", // folder_prefix_legacy
        "checkpoint.page" // folder_prefix_checkpoint
    };

    std::vector<String> pagefile_names;
    for (const auto & prefix : folder_prefixs)
    {
        pagefile_names.emplace_back(fmt::format("/{}_{}_{}", prefix, entry.file_id, entry.level));
    }

    // We can't found path by delegator
    // Casue current delegator is not runtime delegator
    // So we need search the file in paths.
    for (const auto & path : delegator->listPaths())
    {
        for (const auto & pf_name : pagefile_names)
        {
            Poco::File pf(path + pf_name);
            if (pf.exists())
            {
                char * data_buf = (char *)malloc(entry.size);
                if (data_buf == nullptr)
                {
                    throw Exception("OOM happened.", ErrorCodes::LOGICAL_ERROR);
                }

                RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(pf.path(), -1, nullptr);
                PageUtil::readFile(file_for_read, entry.offset, data_buf, entry.size, nullptr);
                return data_buf;
            }
        }
    }

    // throw Exception(fmt::format("Can't found PageFile [id={},level={}]",entry.file_id,entry.level), ErrorCodes::LOGICAL_ERROR);
}

WriteBatch PageConverter::record2WriteBatch(const EditRecordsV2 & records)
{
    WriteBatch wb;

    for (const auto & record : records)
    {
        switch (record.type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT:
        {
            // TODO
            char * data_buf = readV2data(record.entry);
            // FIXME : maybe not right.
            MemHolder mem_holder = createMemHolder(data_buf, [&](char * p) { free(p); });
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(data_buf, sizeof(data_buf));

            if (record.type == WriteBatch::WriteType::PUT)
            {
                wb.putPage(record.page_id, buff, record.entry.size, record.entry.getPageFieldOffsetChecksums());
            }
            else
            {
                // FIXME : it's not right
                PageFileIdAndLevel id_level;
                id_level.first = record.entry.file_id;
                id_level.second = record.entry.level;
                wb.upsertPage(record.page_id, record.entry.tag, id_level, buff, record.entry.size, record.entry.getPageFieldOffsetChecksums());
            }
        }
        case WriteBatch::WriteType::DEL:
        {
            wb.delPage(record.page_id);
        }
        case WriteBatch::WriteType::REF:
        {
            wb.putRefPage(record.ori_page_id, record.page_id);
        }
        }
    }

    return wb;
}

void PageConverter::writeIntoV3(const PageEntriesEditV2 & edits_from_checkpoints, const std::vector<PageEntriesEditV2> & edits)
{
    PageStorage::Config config;
    PageStorageV3 pagestorage("PageConverter", delegator, config, file_provider);

    WriteBatch wb = record2WriteBatch(edits_from_checkpoints.getRecords());
    pagestorage.write(std::move(wb));

    for (const auto & edit : edits)
    {
        WriteBatch wb_from_edit = record2WriteBatch(edit.getRecords());
        pagestorage.write(std::move(wb_from_edit));
    }
}

void PageConverter::packV2data()
{
    LOG_INFO(log, "PageConverter begin to pack V2 data.");

    Poco::File packed_file(options.packed_path);
    Poco::File packed_manifest_file(options.packed_path + options.packed_manifest_file_path);
    if (packed_file.exists())
    {
        throw Exception(fmt::format("[packed file={}] already existed.", packed_file.path()),
                        ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        packed_file.createDirectory();
        packed_manifest_file.createFile();
    }

    String manifest;
    for (String path : delegator->listPaths())
    {
        Poco::Path origin_path(path);

        manifest += fmt::format("{}:{}\n", origin_path.makeAbsolute().toString(), origin_path.getFileName());
        std::filesystem::copy(path, packed_file.path() + "/" + origin_path.getFileName(), std::filesystem::copy_options::recursive);
    }

    Poco::FileOutputStream fos(packed_manifest_file.path());
    fos << manifest;
    fos.flush();
    fos.close();

    // TODO : compress the packed_file_path
}

void PageConverter::cleanV2data()
{
    LOG_INFO(log, "PageConverter begin to cleanup V2 data.");
    for (String path : delegator->listPaths())
    {
        Poco::File path_dir(path);
        path_dir.remove(true);
        path_dir.createDirectory();
    }
}

void PageConverter::convertV2toV3()
{
    const auto & [edits_from_checkpoints, edits] = readV2meta();
    // TODO : verify

    if (options.old_data_packed)
    {
        packV2data();
    }

    writeIntoV3(edits_from_checkpoints, edits);

    // cleanV2data();
}

std::pair<PageEntriesEditV2, std::vector<PageEntriesEditV2>> PageConverter::readV2meta()
{
    PageStorageV2::ListPageFilesOption opt;
    opt.remove_tmp_files = false;
    opt.ignore_legacy = false;
    opt.ignore_checkpoint = false;
    opt.remove_invalid_files = true;

    const auto & page_files = PageStorageV2::listAllPageFiles(file_provider, delegator, log->getLog(), opt);

    PageStorageV2::MetaMergingQueue merging_queue;
    for (const auto & page_file : page_files)
    {
        if (!(page_file.getType() == PageFile::Type::Formal || page_file.getType() == PageFile::Type::Legacy
              || page_file.getType() == PageFile::Type::Checkpoint))
        {
            throw Exception(fmt::format("Try to read from {}, got illegal type.", page_file.toString()), ErrorCodes::LOGICAL_ERROR);
        }

        if (auto reader = PageFile::MetaMergingReader::createFrom(const_cast<PageFile &>(page_file));
            reader->hasNext())
        {
            // Read one WriteBatch
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }
        // else the file doesn't contain any valid meta, just skip it.
    }

    PageEntriesEditV2 edits_from_checkpoints;

    std::optional<PageFile> checkpoint_file;
    std::optional<WriteBatch::SequenceID> checkpoint_sequence;
    PS::V2::PageFileSet page_files_to_remove;
    std::tie(checkpoint_file, checkpoint_sequence, page_files_to_remove) = PS::V2::restoreFromCheckpoints(merging_queue, "PageConverter", edits_from_checkpoints, log->getLog());
    (void)checkpoint_file;

    WriteBatch::SequenceID write_batch_seq = 0;
    if (checkpoint_sequence)
    {
        write_batch_seq = *checkpoint_sequence;
    }

    std::vector<PageEntriesEditV2> edits;

    while (!merging_queue.empty())
    {
        auto reader = merging_queue.top();
        merging_queue.pop();

        // If no checkpoint, we apply all edits.
        // Else restored from checkpoint, if checkpoint's WriteBatch sequence number is 0, we need to apply
        // all edits after that checkpoint too. If checkpoint's WriteBatch sequence number is not 0, we
        // apply WriteBatch edits only if its WriteBatch sequence is larger than or equal to checkpoint.
        const auto cur_sequence = reader->writeBatchSequence();
        if (!checkpoint_sequence.has_value() || //
            (checkpoint_sequence.has_value() && (*checkpoint_sequence == 0 || *checkpoint_sequence <= cur_sequence)))
        {
            if (cur_sequence > (write_batch_seq + 1))
            {
                LOG_FMT_WARNING(log, "PageConverter read skip non-continuous sequence from {} to {}, [{}]", write_batch_seq, cur_sequence, reader->toString());
            }

            try
            {
                LOG_FMT_TRACE(log, "PageConverter recovering from {}", reader->toString());
                auto edit = reader->getEdits();
                edits.emplace_back(std::move(edit));
                write_batch_seq = cur_sequence;
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage(fmt::format("(while applying edit to PageConverter with {})", reader->toString()));
                throw;
            }
        }

        if (reader->hasNext())
        {
            // Continue to merge next WriteBatch.
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }
        else
        {
            // Set belonging PageFile's offset and close reader.
            LOG_FMT_TRACE(log, "PageConverter merge done from {}", reader->toString());
            reader->setPageFileOffsets();
        }
    }

    return std::make_pair(std::move(edits_from_checkpoints), std::move(edits));
}

} // namespace DB