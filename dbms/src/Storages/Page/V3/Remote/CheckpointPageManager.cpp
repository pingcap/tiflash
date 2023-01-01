#include <algorithm>

#include <Storages/Page/V3/Remote/CheckpointPageManager.h>

namespace DB::PS::V3
{

std::vector<std::tuple<ReadBufferPtr, size_t, UniversalPageId>> CheckpointPageManager::getAllPageWithPrefix(const String & prefix) const
{
    std::vector<std::tuple<ReadBufferPtr, size_t, UniversalPageId>> results;
    auto records = edit.getRecords();
    auto iter = lower_bound(records.begin(),
                            records.end(),
                            prefix,
                            [](const typename PS::V3::universal::PageDirectoryTrait::PageEntriesEdit::EditRecord & record, const String & prefix) {
                                return record.page_id.toStr() < prefix;
                            });
    while (iter != records.end())
    {
        auto & record = *iter;
        if (startsWith(record.page_id.toStr(), prefix))
        {
            assert(record.ori_page_id.empty());
            const auto & location = record.entry.remote_info->data_location;
            auto buf = std::make_shared<ReadBufferFromFile>(checkpoint_data_dir + *location.data_file_id);
            buf->seek(location.offset_in_file);
            results.emplace_back(std::move(buf), location.size_in_file, record.page_id);
            iter++;
        }
        else
        {
            break;
        }
    }
    return results;
}

std::optional<typename PS::V3::universal::PageDirectoryTrait::EditRecord> CheckpointPageManager::findPageRecord(const UniversalPageId & page_id) const
{
    UniversalPageId target_id = page_id;
    const auto & records = edit.getRecords();
    while (true)
    {
        if (auto iter = lower_bound(records.begin(),
                                    records.end(),
                                    target_id,
                                    [](const typename PS::V3::universal::PageDirectoryTrait::PageEntriesEdit::EditRecord & record, const UniversalPageId & id) {
                                        return record.page_id < id;
                                    }); iter != records.end())
        {
            auto record = *iter;
            if (record.page_id == target_id)
            {
                if (record.ori_page_id.empty())
                {
                    return record;
                }
                else
                {
                    target_id = record.ori_page_id;
                }
            }
            else
            {
                break;
            }
        }
        else
        {
            break;
        }
    }
    return std::nullopt;
}

std::optional<UniversalPageId> CheckpointPageManager::getNormalPageId(const UniversalPageId & page_id, bool ignore_if_not_exist) const
{
    auto maybe_record = findPageRecord(page_id);
    if (maybe_record.has_value())
    {
        auto & record = maybe_record.value();
        return record.page_id;
    }
    else if (!ignore_if_not_exist)
    {
        RUNTIME_CHECK_MSG(false, "Cannot find page id {}", page_id);
    }
    else
    {
        return std::nullopt;
    }
}

// buf, size, size of each fields
std::optional<std::tuple<ReadBufferPtr, size_t, PageFieldSizes>> CheckpointPageManager::getReadBuffer(const UniversalPageId & page_id, bool ignore_if_not_exist) const
{
    PageFieldSizes field_sizes;

    auto maybe_record = findPageRecord(page_id);
    if (maybe_record.has_value())
    {
        auto & record = maybe_record.value();
        const auto & location = record.entry.remote_info->data_location;
        auto buf = std::make_shared<ReadBufferFromFile>(checkpoint_data_dir + *location.data_file_id);
        buf->seek(location.offset_in_file);
        const auto & field_offsets = record.entry.field_offsets;
        for (size_t i = 0; i < field_offsets.size(); i++)
        {
            if (i == field_offsets.size() - 1)
                field_sizes.push_back(location.size_in_file - field_offsets.back().first);
            else
                field_sizes.push_back(field_offsets[i + 1].first - field_offsets[i].first);
        }
        return std::make_tuple(std::move(buf), location.size_in_file, field_sizes);
    }
    else if (!ignore_if_not_exist)
    {
        RUNTIME_CHECK_MSG(false, "Cannot find page id {}", page_id);
    }
    else
    {
        return std::nullopt;
    }
}
}
