#pragma once

#include <chrono>
#include <thread>
#include <common/logger_useful.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Storages/MergeTree/MarkRange.h>


namespace DB
{

/** Cache of uncompressed blocks in fast(er) disk device. thread-safe.
  */
class PersistedCache
{
public:
    PersistedCache(size_t max_size_in_bytes, const std::string & base_path,
        const std::string & persisted_path_setting, size_t min_seconds_to_evit = 120);

    ~PersistedCache();

    // Return true if cache exists and path is redirected
    bool redirectMarksFile(std::string & origin_path, size_t file_marks_count);

    // Write data to new cache file
    bool cacheMarksFile(const std::string & origin_path, size_t file_marks_count);

    // If not all marks are cached, return false and not redirect
    bool redirectDataFile(std::string & origin_path, const MarkRanges & mark_ranges,
        const MarksInCompressedFile & marks, size_t file_marks_count, bool expected_exists);

    // Copy marks data from origin file to mapping hollow file
    bool cacheRangesInDataFile(const std::string & origin_path, const MarkRanges & mark_ranges,
        const MarksInCompressedFile & marks, size_t marks_count, size_t max_buffer_size);

private:
    using OriginPath = std::string;

    struct FileMarksCached
    {
        OriginPath path;
        std::vector<UInt8> status;
        bool operating_mrk;
        bool operating_bin;

        FileMarksCached(const OriginPath & path, size_t marks_count)
            : path(path), status(marks_count), operating_mrk(false), operating_bin(false) {}
    };

    using FilesMarksCached = std::unordered_map<OriginPath, FileMarksCached>;

    using PartOriginPath = std::string;

    using Clock = std::chrono::steady_clock;
    using Timestamp = Clock::time_point;

    struct PartCacheStatus
    {
        PartOriginPath part_path;
        FilesMarksCached files_marks_cached;

        // Occuppied bytes are total written bytes, not sum(all file size of this part),
        //  because there is lots of holes in files.
        std::atomic<size_t> occuppied_bytes{0};
        Timestamp last_used_time;
        bool operating;

        std::mutex part_lock;

        PartCacheStatus(const PartOriginPath & part_path) :
            part_path(part_path), last_used_time(Clock::now()), operating(false) {}
    };

    using PartCacheStatusPtr = std::shared_ptr<PartCacheStatus>;

    using CacheStatus = std::unordered_map<PartOriginPath, PartCacheStatusPtr>;

private:
    // Rename a part dir and wait for future clean up
    void deletePart(const std::string & cache_path);

    // Scan cache dir and delete cached parts which origin parts no longer exists
    void scanUnregisteredParts();

    // If total used cache space nears the quota,
    //  scan all parts (in dir and in cache-status), delete the most unused parts
    //  * Rule 1: the higher level parts aways win(stay longer) compare to the lower level parts
    //  * Rule 2: In the same level, the recent used parts win
    void evictMostUnusedParts();

    // Remove all deleted parts, return removed parts count
    size_t removeDeletedParts();

    // Do GC(remove parts and make some space) routine
    void performGC();

    // Get cache status of the part by a mkr or bin file's path, if status not exists, create one
    PartCacheStatusPtr getPartCacheStatus(const std::string & origin_path, bool create_if_not_exists);

    // Get cache file path from origin path
    bool getCachePath(const std::string & origin_path, bool is_part_path, std::string & cache_path);

    // Check all marks in mark_ranges are cached
    bool isFileMarksAllCached(const FileMarksCached & marks_status, const MarkRanges & mark_ranges,
        const MarksInCompressedFile & marks, size_t file_marks_count);

    // Copy all mark_ranges from origin file to cache file,
    //  the cache file will be a hollow file since not the whole file are written
    bool copyFileRanges(const std::string & origin_path, const std::string & cache_path,
        const MarkRanges & mark_ranges, const MarksInCompressedFile & marks, size_t file_marks_count, size_t max_buffer_size, size_t & written_size);

    // Copy a mark range from origin file to cache file,
    //  the cache file will be a hollow file since not the whole file are written
    bool copyFileRange(const std::string & origin_path, const std::string & cache_path,
        int fd_r, int fd_w, size_t pos, size_t size, char * buffer, size_t buffer_size);

private:
    std::atomic<bool> disabled{false};
    const size_t max_size_in_bytes;
    const size_t min_seconds_to_evit;

    std::string base_path;
    std::vector<std::string> persisted_paths;

    CacheStatus cache_status;
    std::mutex cache_lock;
    std::atomic<size_t> occuppied_bytes{0};

    std::unique_ptr<std::thread> gc_thread;
    std::unique_ptr<std::thread> cleanup_thread;
    std::atomic<bool> gc_cancelled{false};

    Logger * log;
};

static const std::string DeletedDirPrefix = "_deleted_";

using PersistedCachePtr = std::shared_ptr<PersistedCache>;

}
