// Copyright 2024 PingCAP, Inc.
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

#include <Common/PODArray.h>
#include <IO/BaseFile/fwd.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/FileProvider/EncryptionPath.h>
#include <IO/FileProvider/FileProvider_fwd.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/File/ColumnStat.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <common/types.h>

namespace DB::DM
{

namespace tests
{
class DMStoreForSegmentReadTaskTest;
class DMFileMetaV2Test;
} // namespace tests

class DMFile;
class DMFileWriter;
class DMFileV3IncrementWriter;

struct DMFileMetaChangeset
{
    std::unordered_map<ColId, std::vector<dtpb::DMFileIndexInfo>> new_indexes_on_cols;
};

class DMFileMeta
{
public:
    DMFileMeta(
        UInt64 file_id_,
        const String & parent_path_,
        DMFileStatus status_,
        KeyspaceID keyspace_id_,
        DMConfigurationOpt configuration_,
        DMFileFormat::Version format_version_)
        : file_id(file_id_)
        , parent_path(parent_path_)
        , status(status_)
        , keyspace_id(keyspace_id_)
        , configuration(configuration_)
        , log(Logger::get())
        , format_version(format_version_)
    {}

    virtual ~DMFileMeta() = default;

    struct ReadMode
    {
    private:
        static constexpr UInt8 READ_NONE = 0x00;
        static constexpr UInt8 READ_COLUMN_STAT = 0x01;
        static constexpr UInt8 READ_PACK_STAT = 0x02;
        static constexpr UInt8 READ_PACK_PROPERTY = 0x04;

        UInt8 value;

    public:
        explicit ReadMode(UInt8 value_)
            : value(value_)
        {}

        static ReadMode all() { return ReadMode(READ_COLUMN_STAT | READ_PACK_STAT | READ_PACK_PROPERTY); }
        static ReadMode none() { return ReadMode(READ_NONE); }
        // after restore with mode, you can call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMode diskSizeOnly() { return ReadMode(READ_COLUMN_STAT); }
        // after restore with mode, you can call `getRows`, `getBytes` to get memory size of this DMFile,
        // and call `getBytesOnDisk` to get disk size of this DMFile
        static ReadMode memoryAndDiskSize() { return ReadMode(READ_COLUMN_STAT | READ_PACK_STAT); }

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

    enum class BlockType : UInt64
    {
        PackStat = 0,
        PackProperty,
        ColumnStat, // Deprecated, use `ExtendColumnStat` instead
        MergedSubFilePos,
        ExtendColumnStat,
    };
    struct BlockHandle
    {
        BlockType type;
        UInt64 offset;
        UInt64 size;
    };
    static_assert(std::is_standard_layout_v<BlockHandle> && sizeof(BlockHandle) == sizeof(UInt64) * 3);

    struct Footer
    {
        UInt64 checksum_frame_length = 0;
        UInt64 checksum_algorithm = 0;
    };
    static_assert(std::is_standard_layout_v<Footer> && sizeof(Footer) == sizeof(UInt64) * 2);

    using PackStats = PaddedPODArray<PackStat>;
    // `PackProperties` is similar to `PackStats` except it uses protobuf to do serialization
    using PackProperties = dtpb::PackProperties;

    ColumnStats & getColumnStats() { return column_stats; }
    PackProperties & getPackProperties() { return pack_properties; }

    static String metaFileName() { return "meta.txt"; }
    static String packStatFileName() { return "pack"; }
    static String packPropertyFileName() { return "property"; }
    static String configurationFileName() { return "config"; }

    String mergedPath(UInt32 number) const { return subFilePath(mergedFilename(number)); }
    virtual void read(const FileProviderPtr & file_provider, const DMFileMeta::ReadMode & read_meta_mode);
    virtual void finalize(
        WriteBuffer & buffer,
        const FileProviderPtr & file_provider,
        const WriteLimiterPtr & write_limiter);
    virtual String metaPath() const { return subFilePath(metaFileName()); }
    virtual UInt32 metaVersion() const { return 0; }
    /**
     * @brief metaVersion += 1. Returns the new meta version.
     * This is only supported in MetaV2.
     */
    virtual UInt32 bumpMetaVersion(DMFileMetaChangeset &&)
    {
        RUNTIME_CHECK_MSG(false, "MetaV1 cannot bump meta version");
    }
    virtual EncryptionPath encryptionMetaPath() const;
    virtual UInt64 getReadFileSize(ColId col_id, const String & filename) const;


public:
    enum class LocalIndexState
    {
        NoNeed,
        IndexPending,
        IndexBuilt
    };
    virtual std::tuple<LocalIndexState, size_t> getLocalIndexState(ColId, IndexID) const
    {
        RUNTIME_CHECK_MSG(false, "MetaV1 does not support getLocalIndexState");
    }

    // Try to get the local index of given col_id and index_id.
    // Return std::nullopt if
    // - the col_id is not exist in the dmfile
    // - the index has not been built
    virtual std::optional<dtpb::DMFileIndexInfo> getLocalIndex(ColId, IndexID) const
    {
        RUNTIME_CHECK_MSG(false, "MetaV1 does not support getLocalIndex");
    }

protected:
    PackStats pack_stats;
    PackProperties pack_properties;
    ColumnStats column_stats;
    std::unordered_set<ColId> column_indices;
    // The id to construct the file path on disk.
    const UInt64 file_id;
    String parent_path;
    DMFileStatus status;
    const KeyspaceID keyspace_id;
    DMConfigurationOpt configuration; // configuration

    const LoggerPtr log;
    DMFileFormat::Version format_version;

protected:
    static FileNameBase getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }
    String colDataPath(const FileNameBase & file_name_base) const
    {
        return subFilePath(colDataFileName(file_name_base));
    }
    String colIndexPath(const FileNameBase & file_name_base) const
    {
        return subFilePath(colIndexFileName(file_name_base));
    }
    String encryptionBasePath() const;
    EncryptionPath encryptionMergedPath(UInt32 number) const;
    String path() const { return getPathByStatus(parent_path, file_id, status); }
    String subFilePath(const String & file_name) const { return path() + "/" + file_name; }
    static String mergedFilename(UInt32 number) { return fmt::format("{}.merged", number); }
    String packStatPath() const { return subFilePath(packStatFileName()); }
    String packPropertyPath() const { return subFilePath(packPropertyFileName()); }
    String configurationPath() const { return subFilePath(configurationFileName()); }
    EncryptionPath encryptionPackStatPath() const;
    EncryptionPath encryptionPackPropertyPath() const;
    EncryptionPath encryptionConfigurationPath() const;
    UInt64 getFileSize(ColId col_id, const String & filename) const;

private:
    // readMetaV1
    void readColumnStat(const FileProviderPtr & file_provider, size_t size);
    void readPackStat(const FileProviderPtr & file_provider, size_t size);
    void readPackProperty(const FileProviderPtr & file_provider, size_t size);
    void readConfiguration(const FileProviderPtr & file_provider);
    void tryUpgradeColumnStatInMetaV1(const FileProviderPtr & file_provider, DMFileFormat::Version ver);
    void initializeIndices();

    // writeMetaV1
    using OffsetAndSize = std::tuple<size_t, size_t>;
    OffsetAndSize writeMetaToBuffer(WriteBuffer & buffer) const;
    OffsetAndSize writePackPropertyToBuffer(WriteBuffer & buffer, UnifiedDigestBase * digest = nullptr);
    void writeMeta(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);
    void writePackProperty(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);
    void writeConfiguration(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter);

    friend class DMFile;
    friend class DMFileWriter;
    friend class DMFileV3IncrementWriter;
};

using DMFileMetaPtr = std::unique_ptr<DMFileMeta>;

inline ReadBufferFromRandomAccessFile openForRead(
    const FileProviderPtr & file_provider,
    const String & path,
    const EncryptionPath & encryption_path,
    const size_t & file_size)
{
    return ReadBufferFromRandomAccessFileBuilder::build(
        file_provider,
        path,
        encryption_path,
        std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), file_size));
}

} // namespace DB::DM
