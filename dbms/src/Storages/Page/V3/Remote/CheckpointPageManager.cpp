#include <algorithm>

#include <Storages/Page/V3/Remote/CheckpointPageManager.h>

namespace DB::PS::V3
{

std::vector<std::tuple<ReadBufferPtr, size_t, UniversalPageId>> CheckpointPageManager::getAllPageWithPrefix(const String & prefix) const
{
    std::vector<std::tuple<ReadBufferPtr, size_t, UniversalPageId>> results;
    for (const auto & record: edit.getRecords())
    {
        // FIXME: early stop by the order nature of record or use binary search
        if (startsWith(record.page_id.toStr(), prefix))
        {
            const auto & location = record.entry.remote_info->data_location;
            auto buf = std::make_shared<ReadBufferFromFile>(checkpoint_data_dir + *location.data_file_id);
            buf->seek(location.offset_in_file);
            results.emplace_back(std::move(buf), location.size_in_file, record.page_id);
        }
    }
    return results;
}

UniversalPageId CheckpointPageManager::getNormalPageId(const UniversalPageId & page_id) const
{
    UniversalPageId target_id = page_id;
    while (true)
    {
        for (const auto & record: edit.getRecords())
        {
            if (record.page_id == target_id)
            {
                if (record.ori_page_id == "")
                {
                    return record.page_id;
                }
                else
                {
                    target_id = record.ori_page_id;
                }
            }
        }
    }
}

// buf, size, size of each fields
std::tuple<ReadBufferPtr, size_t, PageFieldSizes> CheckpointPageManager::getReadBuffer(const UniversalPageId & page_id) const
{
    PageFieldSizes field_sizes;
    // TODO: support seek to target id or use binary search
    UniversalPageId target_id = page_id;


    while (true)
    {
        for (const auto & record: edit.getRecords())
        {
            if (record.page_id == target_id)
            {
                if (record.ori_page_id.empty())
                {
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
                else
                {
                    target_id = record.ori_page_id;
                }
            }
        }
    }

    return std::make_tuple(nullptr, 0, field_sizes);
}
}
