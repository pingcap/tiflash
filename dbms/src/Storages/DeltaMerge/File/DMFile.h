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

class DMFile : private boost::noncopyable
{
public:
    using ChunkSize = UInt64;
    using Sizes     = PaddedPODArray<ChunkSize>;

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

    static DMFilePtr create(UInt64 file_id, const String & parent_path);
    static DMFilePtr recover(UInt64 file_id, const String & parent_path);

    void writeMeta();
    void readMeta();

    UInt64 fileId() { return file_id; }
    String path() { return parent_path + (status == Status::READABLE ? "/dmf_" : "/.tmp.dmf_") + DB::toString(file_id); }
    String metaPath() { return path() + "/meta.txt"; }
    String splitPath() { return path() + "/split"; }
    String colDataPath(ColId col_id) { return path() + "/" + DB::toString(col_id) + ".dat"; }
    String colIndexPath(ColId col_id) { return path() + "/" + DB::toString(col_id) + ".idx"; }
    String colMarkPath(ColId col_id) { return path() + "/" + DB::toString(col_id) + ".mrk"; }

    const auto & getColumnStat(ColId col_id)
    {
        auto it = column_stats.find(col_id);
        if (it == column_stats.end())
            throw Exception("Column [" + DB::toString(col_id) + "] not found in dm file [" + path() + "]");
        return it->second;
    }

    size_t getRows() { return rows; }
    size_t getBytes()
    {
        // TODO: fix me!
        return 0;
    }
    size_t       getChunks() { return split.size(); }
    const auto & getSplit() { return split; }
    const auto & getColumnStats() { return column_stats; }
    auto         getStatus() { return status; }

private:
    DMFile(UInt64 file_id_, const String & parent_path_, Status status_, Logger * log_)
        : file_id(file_id_), parent_path(parent_path_), status(status_), log(log_)
    {
    }

    void addChunk(size_t chunk_rows)
    {
        rows += chunk_rows;
        split.push_back(chunk_rows);
    }
    void setStatus(Status status_) { status = status_; }

    void finalize();

private:
    UInt64 file_id;
    String parent_path;

    UInt64      rows = 0;
    Sizes       split;
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