#pragma once

#include <mutex>
#include <vector>

#include <boost/noncopyable.hpp>

#include <Poco/File.h>

#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <Storages/Transaction/Region.h>

namespace DB
{

static const std::string REGION_DATA_FILE_SUFFIX = ".rgn";
static const std::string REGION_INDEX_FILE_SUFFIX = ".idx";

class RegionFile;
using RegionFilePtr = std::shared_ptr<RegionFile>;

/// TODO Handle file corruption, e.g. add checksum to index file.
class RegionFile
{
public:
    using RegionAndSizeMap = std::unordered_map<RegionID, size_t>;

    static const UInt32 CURRENT_VERSION;

    class Writer : private boost::noncopyable
    {
    public:
        Writer(RegionFile & region_file);

        ~Writer();

        size_t write(const RegionPtr & region);

    private:
        // It is a reference to file_size in RegionFile, will be updated after write.
        off_t & data_file_size;
        WriteBufferFromFile data_file_buf;
        WriteBufferFromFile index_file_buf;
    };

    class Reader : private boost::noncopyable
    {
    public:
        Reader(RegionFile & region_file);

        struct PersistMeta
        {
            PersistMeta(RegionID region_id_, UInt64 region_size_, HashingWriteBuffer::uint128 hashcode_)
                : region_id(region_id_), region_size(region_size_), hashcode(hashcode_)
            {}

            RegionID region_id;
            UInt64 region_size;
            HashingWriteBuffer::uint128 hashcode;
        };

        void checkHash(std::vector<bool> use);

        RegionID hasNext();
        RegionPtr next();
        void skipNext();

        const std::vector<PersistMeta> & regionMetas() { return metas; }

    private:
        std::string data_path;
        ReadBufferFromFile data_file_buf;

        std::vector<PersistMeta> metas;

        size_t next_region_index = 0;
        size_t next_region_offset = 0;
        PersistMeta * next_region_meta = nullptr;
    };

    RegionFile(UInt64 file_id_, const std::string & parent_path_)
        : file_id(file_id_), parent_path(parent_path_), log(&Logger::get("RegionFile"))
    {
        Poco::File file(dataPath());
        file_size = file.exists() ? file.getSize() : 0;
    }

    UInt64 id() const { return file_id; }
    size_t size() const { return file_size; }
    const RegionAndSizeMap & regionAndSizeMap() const { return regions; }

    Writer createWriter() { return Writer(*this); }
    Reader createReader() { return Reader(*this); }

    /// The one with smaller file_id will be covered.
    /// Return true when we should try next other file with smaller file_id.
    bool tryCoverRegion(RegionID region_id, RegionFile & other);

    /// You should call this method after write/read regions into/outof file.
    /// Return true: already contains region_id.
    bool addRegion(RegionID region_id, size_t region_size);
    // Return true: contains region_id.
    bool dropRegion(RegionID region_id);

    /// Remove underlying file and clean up resources.
    void destroy();

    void resetId(UInt64 new_file_id);

    Float64 useRate();

private:
    std::string dataPath();
    std::string indexPath();
    std::string dataPath(UInt64 the_file_id);
    std::string indexPath(UInt64 the_file_id);

private:
    UInt64 file_id;
    std::string parent_path;

    RegionAndSizeMap regions;

    off_t file_size;

    Logger * log;
};

} // namespace DB
