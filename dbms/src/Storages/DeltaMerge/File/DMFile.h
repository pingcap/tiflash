#pragma once

#include <Core/Types.h>
#include <IO/FileProvider.h>
#include <IO/ReadBufferFromFileProvider.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/ColumnStat.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFileDefines.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;
using DMFiles   = std::vector<DMFilePtr>;

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
    static DMFilePtr restore(const FileProviderPtr & file_provider, UInt64 file_id, UInt64 ref_id, const String & parent_path, bool read_meta = true);

    static std::set<UInt64> listAllInPath(const String & parent_path, bool can_gc);

    bool canGC();
    void enableGC();
    void remove(const FileProviderPtr & file_provider);

    UInt64 fileId() const { return file_id; }
    UInt64 refId() const { return ref_id; }
    String path() const { return parent_path + (status == Status::READABLE ? "/dmf_" : "/.tmp.dmf_") + DB::toString(file_id); }
    String metaPath() const { return path() + "/meta.txt"; }
    String packStatPath() const { return path() + "/pack"; }
    // Do not gc me.
    String ngcPath() const;
    String colDataPath(const String & file_name_base) const { return path() + "/" + file_name_base + ".dat"; }
    String colIndexPath(const String & file_name_base) const { return path() + "/" + file_name_base + ".idx"; }
    String colMarkPath(const String & file_name_base) const { return path() + "/" + file_name_base + ".mrk"; }

    String         encryptionBasePath() const { return parent_path + "/dmf_" + DB::toString(file_id); }
    EncryptionPath encryptionDataPath(const String & file_name_base) const
    {
        return EncryptionPath(encryptionBasePath(), file_name_base + ".dat");
    }
    EncryptionPath encryptionIndexPath(const String & file_name_base) const
    {
        return EncryptionPath(encryptionBasePath(), file_name_base + ".idx");
    }
    EncryptionPath encryptionMarkPath(const String & file_name_base) const
    {
        return EncryptionPath(encryptionBasePath(), file_name_base + ".mrk");
    }
    EncryptionPath encryptionMetaPath() const
    {
        return EncryptionPath(encryptionBasePath(), "meta.txt");
    }
    EncryptionPath encryptionPackStatPath() const
    {
        return EncryptionPath(encryptionBasePath(), "pack");
    }

    size_t getRows() const
    {
        size_t rows = 0;
        for (auto & s : pack_stats)
            rows += s.rows;
        return rows;
    }

    size_t getBytes() const
    {
        size_t bytes = 0;
        for (auto & s : pack_stats)
            bytes += s.bytes;
        return bytes;
    }

    size_t getBytesOnDisk() const
    {
        // This include column data & its index bytes in disk.
        // Not counting DMFile's meta and pack stat, they are usally small enough to ignore.
        size_t bytes = 0;
        for (const auto & c : column_stats)
            bytes += c.second.serialized_bytes;
        return bytes;
    }

    size_t            getPacks() const { return pack_stats.size(); }
    const PackStats & getPackStats() const { return pack_stats; }
    const PackStat &  getPackStat(size_t pack_index) const { return pack_stats[pack_index]; }

    const ColumnStat & getColumnStat(ColId col_id) const
    {
        if (auto it = column_stats.find(col_id); it != column_stats.end())
        {
            return it->second;
        }
        throw Exception("Column [" + DB::toString(col_id) + "] not found in dm file [" + path() + "]");
    }
    bool isColumnExist(ColId col_id) const { return column_stats.find(col_id) != column_stats.end(); }

    Status getStatus() const { return status; }

    static String getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }

private:
    DMFile(UInt64 file_id_, UInt64 ref_id_, const String & parent_path_, Status status_, Logger * log_)
        : file_id(file_id_), ref_id(ref_id_), parent_path(parent_path_), status(status_), log(log_)
    {
    }

    void writeMeta(const FileProviderPtr & file_provider);
    void readMeta(const FileProviderPtr & file_provider);

    void upgradeMetaIfNeed(const FileProviderPtr & file_provider, DMFileVersion ver);

    void addPack(const PackStat & pack_stat) { pack_stats.push_back(pack_stat); }
    void setStatus(Status status_) { status = status_; }

    void finalize(const FileProviderPtr & file_provider);

private:
    UInt64 file_id;
    UInt64 ref_id; // It is a reference to file_id, could be the same.
    String parent_path;

    PackStats   pack_stats;
    ColumnStats column_stats;

    Status status;

    Logger * log;

    friend class DMFileWriter;
    friend class DMFileReader;
};

inline ReadBufferFromFileProvider openForRead(const FileProviderPtr & file_provider, const String & path, const EncryptionPath & encryption_path)
{
    return ReadBufferFromFileProvider(file_provider, path, encryption_path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

} // namespace DM
} // namespace DB
