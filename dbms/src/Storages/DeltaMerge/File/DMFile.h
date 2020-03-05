#pragma once

#include <Poco/File.h>

#include <Core/Types.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/DeltaMerge/ColumnStat.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;
using DMFiles   = std::vector<DMFilePtr>;

static const String NGC_FILE_NAME = "NGC";

class DMFile : private boost::noncopyable
{
public:
    enum Status : int
    {
        WRITABLE,
        WRITING,
        READABLE,
    };

    static String statusString(Status status)
    {
        switch (status)
        {
        case WRITABLE:
            return "WRITABLE";
        case WRITING:
            return "WRITING";
        case READABLE:
            return "READABLE";
        default:
            throw Exception("Unexpected status: " + DB::toString((int)status));
        }
    }

    struct PackStat
    {
        UInt32 rows;
        UInt32 not_clean;
        UInt64 first_version;
        UInt64 bytes;
        UInt8  first_tag;
    };

    using PackStats = PaddedPODArray<PackStat>;

    static DMFilePtr create(UInt64 file_id, const String & parent_path);
    static DMFilePtr restore(UInt64 file_id, UInt64 ref_id, const String & parent_path, bool read_meta = true);

    void writeMeta();
    void readMeta();

    static std::set<UInt64> listAllInPath(const String & parent_path, bool can_gc);

    bool canGC();
    void enableGC();
    void remove();

    UInt64 fileId() { return file_id; }
    UInt64 refId() { return ref_id; }
    String path() { return parent_path + (status == Status::READABLE ? "/dmf_" : "/.tmp.dmf_") + DB::toString(file_id); }
    String metaPath() { return path() + "/meta.txt"; }
    String packStatPath() { return path() + "/pack"; }
    // Do not gc me.
    String ngcPath() { return path() + "/" + NGC_FILE_NAME; }
    String colDataPath(const String & file_name_base) { return path() + "/" + file_name_base + ".dat"; }
    String colIndexPath(const String & file_name_base) { return path() + "/" + file_name_base + ".idx"; }
    String colEdgePath(const String & file_name_base) { return path() + "/" + file_name_base + ".edge"; }
    String colMarkPath(const String & file_name_base) { return path() + "/" + file_name_base + ".mrk"; }

    const ColumnStat & getColumnStat(ColId col_id)
    {
        auto it = column_stats.find(col_id);
        if (it == column_stats.end())
            throw Exception("Column [" + DB::toString(col_id) + "] not found in dm file [" + path() + "]");
        return it->second;
    }

    size_t getRows()
    {
        size_t rows = 0;
        for (auto & s : pack_stats)
            rows += s.rows;
        return rows;
    }

    size_t getBytes()
    {
        size_t bytes = 0;
        for (auto & s : pack_stats)
            bytes += s.bytes;
        return bytes;
    }

    size_t              getPacks() { return pack_stats.size(); }
    const PackStats &  getPackStats() { return pack_stats; }
    const PackStat &   getPackStat(size_t pack_index) { return pack_stats[pack_index]; }
    const ColumnStats & getColumnStats() { return column_stats; }
    Status              getStatus() { return status; }

    static String getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }

private:
    DMFile(UInt64 file_id_, UInt64 ref_id_, const String & parent_path_, Status status_, Logger * log_)
        : file_id(file_id_), ref_id(ref_id_), parent_path(parent_path_), status(status_), log(log_)
    {
    }

    void addPack(const PackStat & pack_stat) { pack_stats.push_back(pack_stat); }
    void setStatus(Status status_) { status = status_; }

    void finalize();

private:
    UInt64 file_id;
    UInt64 ref_id; // It is a reference to file_id, could be the same.
    String parent_path;

    PackStats  pack_stats;
    ColumnStats column_stats;

    Status status;

    Logger * log;

    friend class DMFileWriter;
    friend class DMFileReader;
};

inline ReadBufferFromFile openForRead(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

} // namespace DM
} // namespace DB