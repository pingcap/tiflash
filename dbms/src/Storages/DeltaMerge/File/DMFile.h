#pragma once

#include <Core/Types.h>
#include <Storages/DeltaMerge/ColIdAndType.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;

class DMFile
{
public:
    enum Status : UInt8
    {
        WRITABLE,
        WRITTING,
        READABLE,
    };

    static DMFilePtr create(UInt64 file_id, const String & parent_path);
    static DMFilePtr recover(const String & parent_path, const String & file_name);

    void writeMeta();
    void readMeta();

    String path() { return parent_path + (status == Status::READABLE ? "/dmf_" : "/.tmp.dmf_") + DB::toString(file_id); }

    size_t numChunks() { return (rows + granularity - 1) / granularity; }
    size_t numRowsInChunk(size_t chunk_id)
    {
        auto chunks = numChunks();
        if (chunk_id < chunks - 1)
        {
            return granularity;
        }
        else
        {
            auto n = rows % granularity;
            return n == 0 ? granularity : n;
        }
    }

private:
    DMFile(UInt64 file_id_, const String & parent_path_, Status status_, Logger * log_)
        : file_id(file_id_), //
          parent_path(parent_path_),
          status(status_),
          log(log_)
    {
    }

private:
    UInt64 file_id;
    String parent_path;

    UInt64          rows;
    UInt64          granularity;
    ColIdAndTypeSet colid_and_types;

    std::atomic<Status> status;

    Logger * log;

    friend class DMFileWriter;
    friend class DMFileReader;
};

} // namespace DM
} // namespace DB