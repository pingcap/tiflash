#include <Common/Exception.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>

namespace DB::PS::V3
{

void CPDataFilesStatCache::updateValidSize(const RemoteFileValidSizes & valid_sizes)
{
    std::lock_guard lock(mtx);
    // If the file is not exist in the latest valid_sizes, then we can
    // remove the cache
    for (auto iter = stats.begin(); iter != stats.end(); /*empty*/)
    {
        const auto & file_id = iter->first;
        if (!valid_sizes.contains(file_id))
        {
            iter = stats.erase(iter);
        }
        else
        {
            ++iter;
        }
    }
    // If the file_id exists, write down the valid size, keep total_size unchanged.
    // Else create a cache entry to write down valid size but leave total_size == 0.
    for (const auto & [file_id, valid_size] : valid_sizes)
    {
        auto res = stats.try_emplace(file_id, CPDataFileStat{});
        res.first->second.valid_size = valid_size;
    }
}

void CPDataFilesStatCache::updateTotalSize(const std::unordered_map<String, CPDataFileStat> & total_sizes)
{
    std::lock_guard lock(mtx);
    for (const auto & [file_id, new_stat] : total_sizes)
    {
        // Some file_ids may be erased before `updateTotalSize`, but it is
        // safe to ignore them.
        // Ony update the file_ids that still valid is OK.
        if (auto iter = stats.find(file_id); iter != stats.end())
        {
            iter->second.total_size = new_stat.total_size;
        }
    }
}

} // namespace DB::PS::V3
