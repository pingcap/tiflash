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
    using ChunkSize = UInt64;
    using Sizes     = std::vector<ChunkSize>;

    enum Status
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
    String metaPath() { return path() + "/meta.txt"; }
    String splitPath() { return path() + "/split.dat"; }
    String colDataPath(ColId col_id) { return path() + "/" + DB::toString(col_id) + ".col"; }
    String colMarkPath(ColId col_id) { return path() + "/" + DB::toString(col_id) + ".mark"; }

    const auto & chunkSizes() { return chunk_sizes; }
    const auto & colIdAndTypes() { return colid_and_types; }
    auto         getStatus() { return status; }

private:
    DMFile(UInt64 file_id_, const String & parent_path_, Status status_, Logger * log_)
        : file_id(file_id_), //
          parent_path(parent_path_),
          status(status_),
          log(log_)
    {
    }

    void setChunkSizes(const Sizes & chunk_sizes_) { chunk_sizes = chunk_sizes_; }
    void setColIdAndTypes(const ColIdAndTypeSet & colid_and_types_) { colid_and_types = colid_and_types_; }
    void setStatus(Status status_) { status = status_; }

private:
    UInt64 file_id;
    String parent_path;

    UInt64          rows;
    Sizes           chunk_sizes;
    ColIdAndTypeSet colid_and_types;

    Status status;

    Logger * log;

    friend class DMFileWriter;
    friend class DMFileReader;
};

} // namespace DM
} // namespace DB