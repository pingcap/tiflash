#pragma once

#include <functional>
#include <map>

#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionFile.h>

#include <Common/PersistedContainer.h>

namespace DB
{

// TODO: move to Defines.h
// 20 MB.
#define DBMS_REGION_FILE_SIZE 209715200
#define DBMS_REGION_FILE_MAX_SIZE 2097152000
#define DBMS_REGION_FILE_SMALL_SIZE 2097152ULL

// Make sure it is the largest file id.
static constexpr UInt64 CURRENT_REGION_FILE_ID = std::numeric_limits<UInt64>::max();
static const std::string VALID_REGIONS_FILE_NAME = "regions";

using RegionMap = std::unordered_map<RegionID, RegionPtr>;

// TODO: use RegionID instead of UInt64

class RegionPersister final : private boost::noncopyable
{
public:
    // FileMap has to be a sorted map, with file_id descending order.
    using FileMap = std::map<UInt64, RegionFilePtr, std::greater<>>;

    struct Config
    {
        // = default; <--- clang bug
        Config() {}

        size_t file_size = DBMS_REGION_FILE_SIZE;
        size_t file_max_size = DBMS_REGION_FILE_MAX_SIZE;
        size_t file_small_size = DBMS_REGION_FILE_SMALL_SIZE;

        Float64 merge_hint_low_used_rate = 0.35;
        size_t  merge_hint_low_used_file_total_size = DBMS_REGION_FILE_SIZE;
        size_t  merge_hint_low_used_file_num = 10;
    };

    RegionPersister(const std::string & path_, const Config config_ = {}) :
        path(path_), config(config_),
        valid_regions(path + VALID_REGIONS_FILE_NAME), log(&Logger::get("RegionPersister"))
    {
        valid_regions.restore();

        Poco::File dir(path);
        if (!dir.exists())
            dir.createDirectories();
    }

    void drop(UInt64 region_id);
    void persist(const RegionPtr & region);
    void restore(RegionMap &, const Region::RegionClientCreateFunc &);
    bool gc();

    // For tests.
    FileMap _getFiles()
    {
        std::lock_guard<std::mutex> map_lock(region_map_mutex);
        return files;
    }

private:
    // Current file is the one which to persist regions always append into.
    // It's file id is CURRENT_REGION_FILE_ID, which is a very large id to make sure it is larger than other files.
    // Current file will be converted into normal file by reset it's file_id to max_file_id + 1, when it is big enough.
    RegionFile * getOrCreateCurrentFile();

    // GC file is used to merge those files with lots of outdated regions.
    // Their file_id are max_file_id + 1.
    RegionFile * createGCFile();

    // Cover region in older files.
    void coverOldRegion(RegionFile * file, UInt64 region_id);

private:
    const std::string path;
    const Config config{};

    FileMap files;
    // except current file.
    UInt64 max_file_id = 0;
    // Protect all above
    std::mutex region_map_mutex;

    PersistedUnorderedUInt64Set valid_regions;
    // Protect persist_mutex
    std::mutex persist_mutex;

    Logger * log;
};

} // namespace DB
