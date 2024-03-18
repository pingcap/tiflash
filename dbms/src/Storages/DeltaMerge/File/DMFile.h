// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Core/Types.h>
#include <Encryption/FileProvider.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/ColumnStat.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
#include <Storages/DeltaMerge/File/dtpb/dmfile.pb.h>
#include <Storages/FormatVersion.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <common/logger_useful.h>
namespace DB::DM
{
class DMFile;
}
namespace DTTool::Migrate
{
struct MigrateArgs;
bool isRecognizable(const DB::DM::DMFile & file, const std::string & target);
bool needFrameMigration(const DB::DM::DMFile & file, const std::string & target);
int migrateServiceMain(DB::Context & context, const MigrateArgs & args);
} // namespace DTTool::Migrate

namespace DB
{
namespace DM
{

class DMFileWithVectorIndexBlockInputStream;

using DMFilePtr = std::shared_ptr<DMFile>;
using DMFiles = std::vector<DMFilePtr>;

class DMFile : private boost::noncopyable
{
    friend class DMFileWithVectorIndexBlockInputStream;

public:
    enum Status : int
    {
        WRITABLE,
        WRITING,
        READABLE,
        DROPPED,
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
            throw Exception("Unexpected status: " + DB::toString(static_cast<int>(status)));
        }
    }

    struct ReadMetaMode
    {
    private:
        static constexpr size_t READ_NONE = 0x00;
        static constexpr size_t READ_COLUMN_STAT = 0x01;
        static constexpr size_t READ_PACK_STAT = 0x02;
        static constexpr size_t READ_PACK_PROPERTY = 0x04;

        size_t value;

    public:
        explicit ReadMetaMode(size_t value_)
            : value(value_)
        {}

        static ReadMetaMode all() { return ReadMetaMode(READ_COLUMN_STAT | READ_PACK_STAT | READ_PACK_PROPERTY); }
        static ReadMetaMode none() { return ReadMetaMode(READ_NONE); }
        // after restore with mode, you can call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMetaMode diskSizeOnly() { return ReadMetaMode(READ_COLUMN_STAT); }
        // after restore with mode, you can call `getRows`, `getBytes` to get memory size of this DMFile,
        // and call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMetaMode memoryAndDiskSize() { return ReadMetaMode(READ_COLUMN_STAT | READ_PACK_STAT); }

        inline bool needColumnStat() const { return value & READ_COLUMN_STAT; }
        inline bool needPackStat() const { return value & READ_PACK_STAT; }
        inline bool needPackProperty() const { return value & READ_PACK_PROPERTY; }

        inline bool isNone() const { return value == READ_NONE; }
        inline bool isAll() const { return needColumnStat() && needPackStat() && needPackProperty(); }
    };

    struct PackStat
    {
        UInt32 rows;
        UInt32 not_clean;
        UInt64 first_version;
        UInt64 bytes;
        UInt8 first_tag;

        String toDebugString() const
        {
            return fmt::format(
                "rows={}, not_clean={}, first_version={}, bytes={}, first_tag={}",
                rows,
                not_clean,
                first_version,
                bytes,
                first_tag);
        }
    };
    static_assert(std::is_standard_layout_v<PackStat>);

    struct PackProperty
    {
        // when gc_safe_point exceed this version, there must be some data obsolete in this pack
        UInt64 gc_hint_version{};
        // effective rows(multiple versions of one row is count as one include delete)
        UInt64 num_rows{};
        // the number of rows in this pack which are deleted
        UInt64 deleted_rows{};

        void toProtobuf(dtpb::PackProperty * p) const
        {
            p->set_gc_hint_version(gc_hint_version);
            p->set_num_rows(num_rows);
            p->set_deleted_rows(deleted_rows);
        }

        void fromProtoBuf(const dtpb::PackProperty & p)
        {
            gc_hint_version = p.gc_hint_version();
            num_rows = p.num_rows();
            deleted_rows = p.deleted_rows();
        }

        explicit PackProperty(const dtpb::PackProperty & p) { fromProtoBuf(p); }
    };
    static_assert(std::is_standard_layout_v<PackProperty>);

    enum class MetaBlockType : UInt64
    {
        PackStat = 0,
        PackProperty,
        ColumnStat, // Deprecated, use `ExtendColumnStat` instead
        MergedSubFilePos,
        ExtendColumnStat,
    };
    struct MetaBlockHandle
    {
        MetaBlockType type;
        UInt64 offset;
        UInt64 size;
    };
    static_assert(std::is_standard_layout_v<MetaBlockHandle> && sizeof(MetaBlockHandle) == sizeof(UInt64) * 3);

    struct MetaFooter
    {
        UInt64 checksum_frame_length = 0;
        UInt64 checksum_algorithm = 0;
    };
    static_assert(std::is_standard_layout_v<MetaFooter> && sizeof(MetaFooter) == sizeof(UInt64) * 2);

    struct MetaPackInfo
    {
        UInt64 pack_property_offset;
        UInt64 pack_property_size;
        UInt64 column_stat_offset;
        UInt64 column_stat_size;
        UInt64 pack_stat_offset;
        UInt64 pack_stat_size;

        MetaPackInfo()
            : pack_property_offset(0)
            , pack_property_size(0)
            , column_stat_offset(0)
            , column_stat_size(0)
            , pack_stat_offset(0)
            , pack_stat_size(0)
        {}
    };

    struct Footer
    {
        MetaPackInfo meta_pack_info;
        UInt64 sub_file_stat_offset;
        UInt32 sub_file_num;

        Footer()
            : sub_file_stat_offset(0)
            , sub_file_num(0)
        {}
    };

    using PackStats = PaddedPODArray<PackStat>;
    // `PackProperties` is similar to `PackStats` except it uses protobuf to do serialization
    using PackProperties = dtpb::PackProperties;


    // Normally, we use STORAGE_FORMAT_CURRENT to determine whether use meta v2.
    static DMFilePtr create(
        UInt64 file_id,
        const String & parent_path,
        DMConfigurationOpt configuration = std::nullopt,
        UInt64 small_file_size_threshold = 128 * 1024,
        UInt64 merged_file_max_size = 16 * 1024 * 1024,
        DMFileFormat::Version = STORAGE_FORMAT_CURRENT.dm_file);

    static DMFilePtr restore(
        const FileProviderPtr & file_provider,
        UInt64 file_id,
        UInt64 page_id,
        const String & parent_path,
        const ReadMetaMode & read_meta_mode);

    struct ListOptions
    {
        // Only return the DTFiles id list that can be GC
        bool only_list_can_gc = true;
        // Try to clean up temporary / dropped files
        bool clean_up = false;
    };
    static std::vector<String> listLocal(const String & parent_path);
    static std::vector<String> listS3(const String & parent_path);
    static std::set<UInt64> listAllInPath(
        const FileProviderPtr & file_provider,
        const String & parent_path,
        const ListOptions & options);

    // static helper function for getting path
    static String getPathByStatus(const String & parent_path, UInt64 file_id, DMFile::Status status);
    static String getNGCPath(const String & parent_path, UInt64 file_id, DMFile::Status status);

    bool canGC() const;
    void enableGC() const;
    void remove(const FileProviderPtr & file_provider);

    // The ID for locating DTFile on disk
    UInt64 fileId() const { return file_id; }
    // The PageID for locating this object in the StoragePool.data
    UInt64 pageId() const { return page_id; }

    String path() const;

    const String & parentPath() const { return parent_path; }

    size_t getRows() const
    {
        size_t rows = 0;
        for (const auto & s : pack_stats)
            rows += s.rows;
        return rows;
    }

    size_t getBytes() const
    {
        size_t bytes = 0;
        for (const auto & s : pack_stats)
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

    size_t getPacks() const { return pack_stats.size(); }
    const PackStats & getPackStats() const { return pack_stats; }
    PackProperties & getPackProperties() { return pack_properties; }

    const ColumnStat & getColumnStat(ColId col_id) const
    {
        if (auto it = column_stats.find(col_id); likely(it != column_stats.end()))
        {
            return it->second;
        }
        throw Exception("Column [" + DB::toString(col_id) + "] not found in dm file [" + path() + "]");
    }
    bool isColumnExist(ColId col_id) const { return column_stats.find(col_id) != column_stats.end(); }

    /*
     * TODO: This function is currently unused. We could use it when:
     *   1. The content is polished (e.g. including at least file ID, and use a format easy for grep).
     *   2. Unify the place where we are currently printing out DMFile's `path` or `file_id`.
     */
    // String toString() const
    // {
    //     return "{DMFile, packs: " + DB::toString(getPacks()) + ", rows: " + DB::toString(getRows()) + ", bytes: " + DB::toString(getBytes())
    //         + ", file size: " + DB::toString(getBytesOnDisk()) + "}";
    // }

    DMConfigurationOpt & getConfiguration() { return configuration; }

    /**
     * Return all column defines. This is useful if you want to read all data from a dmfile.
     * Note that only the column id and type is valid.
     * @return All columns
     */
    ColumnDefines getColumnDefines()
    {
        ColumnDefines results{};
        results.reserve(this->column_stats.size());
        for (const auto & cs : this->column_stats)
        {
            results.emplace_back(cs.first, "", cs.second.type);
        }
        return results;
    }

    static String metav2FileName() { return "meta"; }
    std::vector<String> listFilesForUpload();
    void switchToRemote(const S3::DMFileOID & oid);

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    DMFile(
        UInt64 file_id_,
        UInt64 page_id_,
        String parent_path_,
        Status status_,
        UInt64 small_file_size_threshold_ = 128 * 1024,
        UInt64 merged_file_max_size_ = 16 * 1024 * 1024,
        DMConfigurationOpt configuration_ = std::nullopt,
        DMFileFormat::Version version_ = STORAGE_FORMAT_CURRENT.dm_file)
        : file_id(file_id_)
        , page_id(page_id_)
        , parent_path(std::move(parent_path_))
        , status(status_)
        , configuration(std::move(configuration_))
        , log(Logger::get())
        , version(version_)
        , small_file_size_threshold(small_file_size_threshold_)
        , merged_file_max_size(merged_file_max_size_)
    {}

    // Do not gc me.
    String ngcPath() const;
    String metaPath() const { return subFilePath(metaFileName()); }
    String packStatPath() const { return subFilePath(packStatFileName()); }
    String packPropertyPath() const { return subFilePath(packPropertyFileName()); }
    String configurationPath() const { return subFilePath(configurationFileName()); }
    String metav2Path() const { return subFilePath(metav2FileName()); }
    String mergedPath(UInt32 number) const { return subFilePath(mergedFilename(number)); }

    using FileNameBase = String;
    size_t colIndexSizeByName(const FileNameBase & file_name_base) const
    {
        return Poco::File(colIndexPath(file_name_base)).getSize();
    }
    size_t colDataSizeByName(const FileNameBase & file_name_base) const
    {
        return Poco::File(colDataPath(file_name_base)).getSize();
    }
    size_t colIndexSize(ColId id);
    enum class ColDataType
    {
        Elements,
        NullMap,
        ArraySizes,
    };
    size_t colDataSize(ColId id, ColDataType type);

    String colDataPath(const FileNameBase & file_name_base) const
    {
        return subFilePath(colDataFileName(file_name_base));
    }
    String colIndexPath(const FileNameBase & file_name_base) const
    {
        return subFilePath(colIndexFileName(file_name_base));
    }
    String colMarkPath(const FileNameBase & file_name_base) const
    {
        return subFilePath(colMarkFileName(file_name_base));
    }

    String colIndexCacheKey(const FileNameBase & file_name_base) const;
    String colMarkCacheKey(const FileNameBase & file_name_base) const;

    bool isColIndexExist(const ColId & col_id) const;

    String encryptionBasePath() const;
    EncryptionPath encryptionDataPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionIndexPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionMarkPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionMetaPath() const;
    EncryptionPath encryptionPackStatPath() const;
    EncryptionPath encryptionPackPropertyPath() const;
    EncryptionPath encryptionConfigurationPath() const;
    EncryptionPath encryptionMetav2Path() const;
    EncryptionPath encryptionMergedPath(UInt32 number) const;

    static FileNameBase getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }

    static String metaFileName() { return "meta.txt"; }
    static String packStatFileName() { return "pack"; }
    static String packPropertyFileName() { return "property"; }
    static String configurationFileName() { return "config"; }
    static String mergedFilename(UInt32 number) { return fmt::format("{}.merged", number); }

    static String colDataFileName(const FileNameBase & file_name_base);
    static String colIndexFileName(const FileNameBase & file_name_base);
    static String colMarkFileName(const FileNameBase & file_name_base);

    using OffsetAndSize = std::tuple<size_t, size_t>;
    OffsetAndSize writeMetaToBuffer(WriteBuffer & buffer);
    OffsetAndSize writePackStatToBuffer(WriteBuffer & buffer);
    OffsetAndSize writePackPropertyToBuffer(WriteBuffer & buffer, UnifiedDigestBase * digest = nullptr);

    void writeMeta(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);
    void writePackProperty(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);
    void writeConfiguration(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);
    void readColumnStat(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info);
    void readMeta(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info);
    void readPackStat(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info);
    void readPackProperty(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info);
    void readConfiguration(const FileProviderPtr & file_provider);

    void writeMetadata(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);
    void readMetadata(const FileProviderPtr & file_provider, const ReadMetaMode & read_meta_mode);

    void upgradeMetaIfNeed(const FileProviderPtr & file_provider, DMFileFormat::Version ver);

    void addPack(const PackStat & pack_stat) { pack_stats.push_back(pack_stat); }

    Status getStatus() const { return status; }
    void setStatus(Status status_) { status = status_; }

    void finalizeForFolderMode(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);

    String subFilePath(const String & file_name) const { return path() + "/" + file_name; }

    void initializeIndices();

    /* New metadata file format:
     * |Pack Stats|Pack Properties|Column Stats|Pack Stats Handle|Pack Properties Handle|Column Stats Handle|Meta Block Handle Count|DMFile Version|Checksum|MetaFooter|
     * |----------------------------------------Checksum include-----------------------------------------------------------------------------------|
     * `MetaFooter` is saved at the end of the file, with fixed length, it contains checksum algorithm and checksum frame length.
     * First, read `MetaFooter` and `Checksum`, and check data integrity.
     * Second, parse handle and parse corresponding data.
     * `PackStatsHandle`, `PackPropertiesHandle` and `ColumnStatsHandle` are offset and size of `PackStats`, `PackProperties` and `ColumnStats`.
     */
    // Meta data is small and 64KB is enough.
    static constexpr size_t meta_buffer_size = 64 * 1024;
    void finalizeMetaV2(WriteBuffer & buffer);
    MetaBlockHandle writeSLPackStatToBuffer(WriteBuffer & buffer);
    MetaBlockHandle writeSLPackPropertyToBuffer(WriteBuffer & buffer);
    MetaBlockHandle writeColumnStatToBuffer(WriteBuffer & buffer);
    MetaBlockHandle writeExtendColumnStatToBuffer(WriteBuffer & buffer);
    MetaBlockHandle writeMergedSubFilePosotionsToBuffer(WriteBuffer & buffer);
    std::vector<char> readMetaV2(const FileProviderPtr & file_provider);
    void parseMetaV2(std::string_view buffer);
    void parseColumnStat(std::string_view buffer);
    void parseExtendColumnStat(std::string_view buffer);
    void parseMergedSubFilePos(std::string_view buffer);
    void parsePackProperty(std::string_view buffer);
    void parsePackStat(std::string_view buffer);
    void finalizeDirName();
    bool useMetaV2() const { return version == DMFileFormat::V3; }

    UInt64 getFileSize(ColId col_id, const String & filename) const;
    UInt64 getReadFileSize(ColId col_id, const String & filename) const;
    UInt64 getMergedFileSizeOfColumn(const MergedSubFileInfo & file_info) const;

    // The id to construct the file path on disk.
    UInt64 file_id;
    // It is the page_id that represent this file in the PageStorage. It could be the same as file id.
    UInt64 page_id;
    String parent_path;

    PackStats pack_stats;
    PackProperties pack_properties;
    ColumnStats column_stats;
    std::unordered_set<ColId> column_indices;

    Status status;
    DMConfigurationOpt configuration; // configuration

    LoggerPtr log;

    DMFileFormat::Version version;

    struct MergedFile
    {
        UInt64 number = 0;
        UInt64 size = 0;
    };

    struct MergedFileWriter
    {
        MergedFile file_info;
        std::unique_ptr<WriteBufferFromWritableFile> buffer;
    };
    PaddedPODArray<MergedFile> merged_files;
    // Filename -> MergedSubFileInfo
    std::unordered_map<String, MergedSubFileInfo> merged_sub_file_infos;

    UInt64 small_file_size_threshold;
    UInt64 merged_file_max_size;

    void finalizeSmallFiles(
        MergedFileWriter & writer,
        FileProviderPtr & file_provider,
        WriteLimiterPtr & write_limiter);
    // check if the size of merged file is larger then the threshold. If so, create a new merged file.
    void checkMergedFile(MergedFileWriter & writer, FileProviderPtr & file_provider, WriteLimiterPtr & write_limiter);

    friend class DMFileWriter;
    friend class DMFileWriterRemote;
    friend class DMFileReader;
    friend class DMFilePackFilter;
    friend class DMFileBlockInputStreamBuilder;
    friend int ::DTTool::Migrate::migrateServiceMain(
        DB::Context & context,
        const ::DTTool::Migrate::MigrateArgs & args);
    friend bool ::DTTool::Migrate::isRecognizable(const DB::DM::DMFile & file, const std::string & target);
    friend bool ::DTTool::Migrate::needFrameMigration(const DB::DM::DMFile & file, const std::string & target);
};

inline ReadBufferFromFileProvider openForRead(
    const FileProviderPtr & file_provider,
    const String & path,
    const EncryptionPath & encryption_path,
    const size_t & file_size)
{
    return ReadBufferFromFileProvider(
        file_provider,
        path,
        encryption_path,
        std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), file_size));
}

} // namespace DM
} // namespace DB
