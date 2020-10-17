#pragma once

#include <Core/Types.h>
#include <Encryption/FileProvider.h>
#include <Encryption/ReadBufferFromFileProvider.h>
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
    enum Mode : int
    {
        SINGLE_FILE,
        FOLDER,
    };

    enum Status : int
    {
        WRITABLE,
        WRITING,
        READABLE,
        DROPPED,
    };

    enum DMSingleFileFormatVersion : int
    {
        SINGLE_FILE_VERSION_BASE = 0,
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
        case DROPPED:
            return "DROPPED";
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

    struct SubFileStat
    {
        SubFileStat() = default;
        SubFileStat(UInt64 offset_, UInt64 size_) : offset{offset_}, size{size_} {}
        UInt64 offset;
        UInt64 size;
    };
    using SubFileStats = std::unordered_map<String, SubFileStat>;

    struct MetaPackInfo
    {
        UInt64 meta_offset;
        UInt64 meta_size;
        UInt64 pack_stat_offset;
        UInt64 pack_stat_size;
    };

    struct Footer
    {
        MetaPackInfo meta_pack_info;
        UInt64       sub_file_stat_offset;
        UInt32       sub_file_num;

        DMSingleFileFormatVersion file_format_version;
    };

    using PackStats = PaddedPODArray<PackStat>;

    static DMFilePtr create(UInt64 file_id, const String & parent_path, bool single_file_mode = false);

    static DMFilePtr
    restore(const FileProviderPtr & file_provider, UInt64 file_id, UInt64 ref_id, const String & parent_path, bool read_meta = true);

    static std::set<UInt64> listAllInPath(const FileProviderPtr & file_provider, const String & parent_path, bool can_gc);

    bool canGC();
    void enableGC();
    void remove(const FileProviderPtr & file_provider);

    UInt64 fileId() const { return file_id; }
    UInt64 refId() const { return ref_id; }

    String path() const;
    String metaPath() const { return subFilePath(metaFileName()); }
    String packStatPath() const { return subFilePath(packStatFileName()); }

    // Do not gc me.
    String ngcPath() const;

    using FileNameBase = String;
    String colDataPath(const FileNameBase & file_name_base) const { return subFilePath(colDataFileName(file_name_base)); }
    String colIndexPath(const FileNameBase & file_name_base) const { return subFilePath(colIndexFileName(file_name_base)); }
    String colMarkPath(const FileNameBase & file_name_base) const { return subFilePath(colMarkFileName(file_name_base)); }

    String colIndexCacheKey(const FileNameBase & file_name_base) const;
    String colMarkCacheKey(const FileNameBase & file_name_base) const;

    size_t colIndexOffset(const FileNameBase & file_name_base) const { return subFileOffset(colIndexFileName(file_name_base)); }
    size_t colMarkOffset(const FileNameBase & file_name_base) const { return subFileOffset(colMarkFileName(file_name_base)); }
    size_t colIndexSize(const FileNameBase & file_name_base) const { return subFileSize(colIndexFileName(file_name_base)); }
    size_t colMarkSize(const FileNameBase & file_name_base) const { return subFileSize(colMarkFileName(file_name_base)); }
    size_t colDataSize(const FileNameBase & file_name_base) const { return subFileSize(colDataFileName(file_name_base)); }

    bool isColIndexExist(const ColId & col_id) const;

    const String         encryptionBasePath() const;
    const EncryptionPath encryptionDataPath(const FileNameBase & file_name_base) const;
    const EncryptionPath encryptionIndexPath(const FileNameBase & file_name_base) const;
    const EncryptionPath encryptionMarkPath(const FileNameBase & file_name_base) const;
    const EncryptionPath encryptionMetaPath() const;
    const EncryptionPath encryptionPackStatPath() const;

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

    static FileNameBase getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }

private:
    DMFile(UInt64 file_id_, UInt64 ref_id_, const String & parent_path_, Mode mode_, Status status_, Logger * log_)
        : file_id(file_id_), ref_id(ref_id_), parent_path(parent_path_), mode(mode_), status(status_), log(log_)
    {
    }

    bool isSingleFileMode() const { return mode == Mode::SINGLE_FILE; }
    bool isFolderMode() const { return mode == Mode::FOLDER; }

    static String metaFileName() { return "meta.txt"; }
    static String packStatFileName() { return "pack"; }
    static String colDataFileName(const FileNameBase & file_name_base) { return file_name_base + ".dat"; }
    static String colIndexFileName(const FileNameBase & file_name_base) { return file_name_base + ".idx"; }
    static String colMarkFileName(const FileNameBase & file_name_base) { return file_name_base + ".mrk"; }

    std::tuple<size_t, size_t> writeMeta(WriteBuffer & buffer);
    std::tuple<size_t, size_t> writePack(WriteBuffer & buffer);

    void writeMeta(const FileProviderPtr & file_provider);
    void readMeta(const FileProviderPtr & file_provider);

    void upgradeMetaIfNeed(const FileProviderPtr & file_provider, DMFileVersion ver);

    void addPack(const PackStat & pack_stat) { pack_stats.push_back(pack_stat); }
    void setStatus(Status status_) { status = status_; }

    void initializeSubFileStatIfNeeded(const FileProviderPtr & file_provider);

    void finalize(const FileProviderPtr & file_provider);
    void finalize(WriteBuffer & buffer);

    void addSubFileStat(const String & name, UInt64 offset, UInt64 size) { sub_file_stats.emplace(name, SubFileStat{offset, size}); }

    const SubFileStat & getSubFileStat(const String & name) const { return sub_file_stats.at(name); }

    bool isSubFileExists(const String & name) const { return sub_file_stats.find(name) != sub_file_stats.end(); }

    const String subFilePath(const String & file_name) const { return isSingleFileMode() ? path() : path() + "/" + file_name; }

    size_t subFileOffset(const String & file_name) const { return isSingleFileMode() ? getSubFileStat(file_name).offset : 0; }

    size_t subFileSize(const String & file_name) const
    {
        return isSingleFileMode() ? getSubFileStat(file_name).size : Poco::File(subFilePath(file_name)).getSize();
    }

private:
    UInt64 file_id;
    UInt64 ref_id; // It is a reference to file_id, could be the same.
    String parent_path;

    PackStats   pack_stats;
    ColumnStats column_stats;

    Mode   mode;
    Status status;

    mutable std::mutex mutex;
    SubFileStats       sub_file_stats;

    Logger * log;

    friend class DMFileWriter;
    friend class DMFileReader;
};

inline ReadBufferFromFileProvider
openForRead(const FileProviderPtr & file_provider, const String & path, const EncryptionPath & encryption_path, const size_t & file_size)
{
    return ReadBufferFromFileProvider(
        file_provider, path, encryption_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), file_size));
}

} // namespace DM
} // namespace DB
