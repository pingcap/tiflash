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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <dmfile.pb.h>
#pragma GCC diagnostic pop

#include <Core/Types.h>
#include <Encryption/FileProvider.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/ColumnStat.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/FormatVersion.h>
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
using DMFilePtr = std::shared_ptr<DMFile>;
using DMFiles = std::vector<DMFilePtr>;

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

        static ReadMetaMode all()
        {
            return ReadMetaMode(READ_COLUMN_STAT | READ_PACK_STAT | READ_PACK_PROPERTY);
        }
        static ReadMetaMode none()
        {
            return ReadMetaMode(READ_NONE);
        }
        // after restore with mode, you can call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMetaMode diskSizeOnly()
        {
            return ReadMetaMode(READ_COLUMN_STAT);
        }
        // after restore with mode, you can call `getRows`, `getBytes` to get memory size of this DMFile,
        // and call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMetaMode memoryAndDiskSize()
        {
            return ReadMetaMode(READ_COLUMN_STAT | READ_PACK_STAT);
        }

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
    };

    struct SubFileStat
    {
        SubFileStat()
            : SubFileStat(0, 0)
        {}
        SubFileStat(UInt64 offset_, UInt64 size_)
            : offset{offset_}
            , size{size_}
        {}
        UInt64 offset;
        UInt64 size;
    };
    using SubFileStats = std::unordered_map<String, SubFileStat>;

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

        DMSingleFileFormatVersion file_format_version;

        Footer()
            : sub_file_stat_offset(0)
            , sub_file_num(0)
            , file_format_version(DMSingleFileFormatVersion::SINGLE_FILE_VERSION_BASE)
        {}
    };

    using PackStats = PaddedPODArray<PackStat>;
    // `PackProperties` is similar to `PackStats` except it uses protobuf to do serialization
    using PackProperties = dtpb::PackProperties;

    static DMFilePtr
    create(UInt64 file_id, const String & parent_path, bool single_file_mode = false, DMConfigurationOpt configuration = std::nullopt);

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
    static std::set<UInt64> listAllInPath(const FileProviderPtr & file_provider, const String & parent_path, const ListOptions & options);

    // static helper function for getting path
    static String getPathByStatus(const String & parent_path, UInt64 file_id, DMFile::Status status);
    static String getNGCPath(const String & parent_path, UInt64 file_id, DMFile::Status status, bool is_single_mode);

    bool canGC();
    void enableGC();
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
    bool isSingleFileMode() const { return mode == Mode::SINGLE_FILE; }

    String toString() const
    {
        return "{DMFile, packs: " + DB::toString(getPacks()) + ", rows: " + DB::toString(getRows()) + ", bytes: " + DB::toString(getBytes())
            + ", file size: " + DB::toString(getBytesOnDisk()) + "}";
    }

    DMConfigurationOpt & getConfiguration() { return configuration; }

    /**
     * Return all column defines. This is useful if you want to read all data from a dmfile.
     * @return All columns
     */
    ColumnDefines getColumnDefines()
    {
        ColumnDefines results{};
        results.reserve(this->column_stats.size());
        for (const auto & i : this->column_stats)
        {
            results.emplace_back(i.first, "", i.second.type);
        }
        return results;
    }

private:
    DMFile(UInt64 file_id_,
           UInt64 page_id_,
           String parent_path_,
           Mode mode_,
           Status status_,
           Poco::Logger * log_,
           DMConfigurationOpt configuration_ = std::nullopt)
        : file_id(file_id_)
        , page_id(page_id_)
        , parent_path(std::move(parent_path_))
        , mode(mode_)
        , status(status_)
        , configuration(std::move(configuration_))
        , log(log_)
    {
    }

    bool isFolderMode() const { return mode == Mode::FOLDER; }

    // Do not gc me.
    String ngcPath() const;
    String metaPath() const { return subFilePath(metaFileName()); }
    String packStatPath() const { return subFilePath(packStatFileName()); }
    String packPropertyPath() const { return subFilePath(packPropertyFileName()); }
    String configurationPath() const { return subFilePath(configurationFileName()); }

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

    String encryptionBasePath() const;
    EncryptionPath encryptionDataPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionIndexPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionMarkPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionMetaPath() const;
    EncryptionPath encryptionPackStatPath() const;
    EncryptionPath encryptionPackPropertyPath() const;
    EncryptionPath encryptionConfigurationPath() const;

    static FileNameBase getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }

    static String metaFileName() { return "meta.txt"; }
    static String packStatFileName() { return "pack"; }
    static String packPropertyFileName() { return "property"; }
    static String configurationFileName() { return "config"; }

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
    void finalizeForSingleFileMode(WriteBuffer & buffer);

    void addSubFileStat(const String & name, UInt64 offset, UInt64 size) { sub_file_stats.emplace(name, SubFileStat{offset, size}); }

    bool isSubFileExists(const String & name) const { return sub_file_stats.find(name) != sub_file_stats.end(); }

    String subFilePath(const String & file_name) const { return isSingleFileMode() ? path() : path() + "/" + file_name; }

    size_t subFileOffset(const String & file_name) const { return isSingleFileMode() ? sub_file_stats.at(file_name).offset : 0; }

    size_t subFileSize(const String & file_name) const { return sub_file_stats.at(file_name).size; }

    void initializeSubFileStatsForFolderMode();

    void initializeIndices();

private:
    // The id to construct the file path on disk.
    UInt64 file_id;
    // It is the page_id that represent this file in the PageStorage. It could be the same as file id.
    UInt64 page_id;
    String parent_path;

    PackStats pack_stats;
    PackProperties pack_properties;
    ColumnStats column_stats;
    std::unordered_set<ColId> column_indices;

    Mode mode;
    Status status;
    DMConfigurationOpt configuration; // configuration

    SubFileStats sub_file_stats;

    Poco::Logger * log;

    friend class DMFileWriter;
    friend class DMFileReader;
    friend class DMFilePackFilter;
    friend class DMFileBlockInputStreamBuilder;
    friend int ::DTTool::Migrate::migrateServiceMain(DB::Context & context, const ::DTTool::Migrate::MigrateArgs & args);
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
