#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/gc/restoreFromCheckpoints.h>
#include <Storages/Page/converter/PageConverter.h>
#include <TestUtils/MockDiskDelegator.h>

namespace DB
{
PageConverter::PageConverter(FileProviderPtr file_provider_, PSDiskDelegatorPtr delegator_)
    : delegator(std::move(delegator_))
    , file_provider(file_provider_)
    , log(getLogWithPrefix(nullptr, "PageConverter"))
{
    String paths;
    for (const auto & path : delegator->listPaths())
    {
        paths += path + ", ";
    }
    LOG_FMT_INFO(log, "PageConverter will converter from paths[{}]", paths);
};

void PageConverter::readFromV2()
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

    PS::V2::PageEntriesEdit edit;

    std::optional<PageFile> checkpoint_file;
    std::optional<WriteBatch::SequenceID> checkpoint_sequence;
    PS::V2::PageFileSet page_files_to_remove;
    std::tie(checkpoint_file, checkpoint_sequence, page_files_to_remove) = PS::V2::restoreFromCheckpoints(merging_queue, "PageConverter", edit, log->getLog());
    (void)checkpoint_file;

    WriteBatch::SequenceID write_batch_seq = 0;
    if (checkpoint_sequence)
    {
        write_batch_seq = *checkpoint_sequence;
    }

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
                // LOG_FMT_TRACE(log, "{} recovering from {}", storage_name, reader->toString());
                auto edits = reader->getEdits();
                //////
                ////// TODO : edit;
                //////
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
}

} // namespace DB