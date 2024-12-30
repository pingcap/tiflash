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

#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFileMetaV2.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/DeltaMerge/File/DMFileV3IncrementWriter_fwd.h>
#include <Storages/DeltaMerge/File/DMFile_fwd.h>
#include <Storages/FormatVersion.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <common/logger_useful.h>

namespace DTTool::Migrate
{
struct MigrateArgs;
bool isRecognizable(const DB::DM::DMFile & file, const std::string & target);
bool needFrameMigration(const DB::DM::DMFile & file, const std::string & target);
int migrateServiceMain(DB::Context & context, const MigrateArgs & args);
} // namespace DTTool::Migrate

namespace DB::DM
{
class DMFileWithVectorIndexBlockInputStream;
namespace tests
{
class DMFileTest;
class DMFileMetaV2Test;
class DMStoreForSegmentReadTaskTest;
} // namespace tests


class DMFile : private boost::noncopyable
{
public:
    // Normally, we use STORAGE_FORMAT_CURRENT to determine whether use meta v2.
    static DMFilePtr create(
        UInt64 file_id,
        const String & parent_path,
        DMConfigurationOpt configuration = std::nullopt,
        UInt64 small_file_size_threshold = 128 * 1024,
        UInt64 merged_file_max_size = 16 * 1024 * 1024,
        KeyspaceID keyspace_id = NullspaceID,
        DMFileFormat::Version = STORAGE_FORMAT_CURRENT.dm_file);

    static DMFilePtr restore(
        const FileProviderPtr & file_provider,
        UInt64 file_id,
        UInt64 page_id,
        const String & parent_path,
        const DMFileMeta::ReadMode & read_meta_mode,
        UInt64 meta_version = 0,
        KeyspaceID keyspace_id = NullspaceID);

    static String info(const DMFiles & dm_files);

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
        const ListOptions & options,
        KeyspaceID keyspace_id = NullspaceID);

    bool canGC() const;
    void enableGC() const;
    void remove(const FileProviderPtr & file_provider);

    // The ID for locating DTFile on disk
    UInt64 fileId() const { return meta->file_id; }
    // The PageID for locating this object in the StoragePool.data
    UInt64 pageId() const { return page_id; }
    // keyspaceID
    KeyspaceID keyspaceId() const { return meta->keyspace_id; }

    DMFileFormat::Version version() const { return meta->format_version; }

    String path() const;

    const String & parentPath() const { return meta->parent_path; }

    size_t getRows() const
    {
        size_t rows = 0;
        for (const auto & s : meta->pack_stats)
            rows += s.rows;
        return rows;
    }

    size_t getBytes() const
    {
        size_t bytes = 0;
        for (const auto & s : meta->pack_stats)
            bytes += s.bytes;
        return bytes;
    }

    size_t getBytesOnDisk() const
    {
        // This include column data & its index bytes in disk.
        // Not counting DMFile's meta and pack stat, they are usally small enough to ignore.
        size_t bytes = 0;
        for (const auto & c : meta->column_stats)
            bytes += c.second.serialized_bytes;
        return bytes;
    }

    size_t getPacks() const { return meta->pack_stats.size(); }
    const DMFileMeta::PackStats & getPackStats() const { return meta->pack_stats; }
    const DMFileMeta::PackProperties & getPackProperties() const { return meta->pack_properties; }
    const ColumnStats & getColumnStats() const { return meta->column_stats; }
    const std::unordered_set<ColId> & getColumnIndices() const { return meta->column_indices; }

    // only used in gtest
    void clearPackProperties() const { meta->pack_properties.clear_property(); }

    const ColumnStat & getColumnStat(ColId col_id) const
    {
        if (auto it = meta->column_stats.find(col_id); likely(it != meta->column_stats.end()))
        {
            return it->second;
        }
        throw Exception("Column [" + DB::toString(col_id) + "] not found in dm file [" + path() + "]");
    }
    bool isColumnExist(ColId col_id) const { return meta->column_stats.contains(col_id); }

    std::tuple<DMFileMeta::LocalIndexState, size_t> getLocalIndexState(ColId col_id, IndexID index_id) const
    {
        return meta->getLocalIndexState(col_id, index_id);
    }

    // Check whether the local index of given col_id and index_id has been built on this dmfile.
    // Return false if
    // - the col_id is not exist in the dmfile
    // - the index has not been built
    bool isLocalIndexExist(ColId col_id, IndexID index_id) const
    {
        return std::get<0>(meta->getLocalIndexState(col_id, index_id)) == DMFileMeta::LocalIndexState::IndexBuilt;
    }

    // Try to get the local index of given col_id and index_id.
    // Return std::nullopt if
    // - the col_id is not exist in the dmfile
    // - the index has not been built
    std::optional<dtpb::VectorIndexFileProps> getLocalIndex(ColId col_id, IndexID index_id) const
    {
        return meta->getLocalIndex(col_id, index_id);
    }

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

    const DMConfigurationOpt & getConfiguration() const { return meta->configuration; }

    /**
     * Return all column defines. This is useful if you want to read all data from a dmfile.
     * Note that only the column id and type is valid.
     * @return All columns
     */
    ColumnDefines getColumnDefines(bool sort_by_id = true) const
    {
        ColumnDefines results{};
        results.reserve(this->meta->column_stats.size());
        for (const auto & cs : this->meta->column_stats)
        {
            results.emplace_back(cs.first, "", cs.second.type);
        }
        if (sort_by_id)
            std::sort(results.begin(), results.end(), [](const auto & lhs, const auto & rhs) {
                return lhs.id < rhs.id;
            });
        return results;
    }

    bool useMetaV2() const { return meta->format_version == DMFileFormat::V3; }

    std::vector<String> listFilesForUpload() const;
    void switchToRemote(const S3::DMFileOID & oid) const;

    UInt32 metaVersion() const { return meta->metaVersion(); }

    bool isColIndexExist(const ColId & col_id) const;

private:
    DMFile(
        UInt64 file_id_,
        UInt64 page_id_,
        String parent_path_,
        DMFileStatus status_,
        UInt64 small_file_size_threshold_ = 128 * 1024,
        UInt64 merged_file_max_size_ = 16 * 1024 * 1024,
        DMConfigurationOpt configuration_ = std::nullopt,
        DMFileFormat::Version version_ = STORAGE_FORMAT_CURRENT.dm_file,
        KeyspaceID keyspace_id_ = NullspaceID)
        : page_id(page_id_)
        , log(Logger::get())
    {
        if (version_ == DMFileFormat::V3)
        {
            meta = std::make_unique<DMFileMetaV2>(
                file_id_,
                std::move(parent_path_),
                status_,
                small_file_size_threshold_,
                merged_file_max_size_,
                keyspace_id_,
                configuration_,
                version_,
                /* meta_version= */ 0);
        }
        else
        {
            meta = std::make_unique<DMFileMeta>( //
                file_id_,
                parent_path_,
                status_,
                keyspace_id_,
                configuration_,
                version_);
        }
    }

    // Do not gc me.
    String ngcPath() const;

    String metav2Path(UInt64 meta_version) const { return subFilePath(DMFileMetaV2::metaFileName(meta_version)); }
    UInt64 getReadFileSize(ColId col_id, const String & filename) const
    {
        return meta->getReadFileSize(col_id, filename);
    }

    using FileNameBase = String;
    size_t colIndexSizeByName(const FileNameBase & file_name_base) const
    {
        return Poco::File(colIndexPath(file_name_base)).getSize();
    }
    size_t colDataSizeByName(const FileNameBase & file_name_base) const
    {
        return Poco::File(colDataPath(file_name_base)).getSize();
    }
    size_t colIndexSize(ColId id) const;
    enum class ColDataType
    {
        Elements,
        NullMap,
        ArraySizes,
        StringSizes,
    };
    size_t colDataSize(ColId id, ColDataType type) const;

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

    String encryptionBasePath() const;
    EncryptionPath encryptionDataPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionIndexPath(const FileNameBase & file_name_base) const;
    EncryptionPath encryptionMarkPath(const FileNameBase & file_name_base) const;

    static FileNameBase getFileNameBase(ColId col_id, const IDataType::SubstreamPath & substream = {})
    {
        return IDataType::getFileNameForStream(DB::toString(col_id), substream);
    }

    static String vectorIndexFileName(IndexID index_id) { return fmt::format("idx_{}.vector", index_id); }
    String vectorIndexPath(IndexID index_id) const { return subFilePath(vectorIndexFileName(index_id)); }

    void addPack(const DMFileMeta::PackStat & pack_stat) const { meta->pack_stats.push_back(pack_stat); }

    DMFileStatus getStatus() const { return meta->status; }
    void setStatus(DMFileStatus status_) const { meta->status = status_; }

    void finalize();

    String subFilePath(const String & file_name) const { return path() + "/" + file_name; }

    // It is the page_id that represent this file in the PageStorage. It could be the same as file id.
    const UInt64 page_id;

    LoggerPtr log;

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    DMFileMetaPtr meta;

    friend class DMFileVectorIndexReader;
    friend class DMFileV3IncrementWriter;
    friend class DMFileWriter;
    friend class DMFileVectorIndexWriter;
    friend class DMFileReader;
    friend class MarkLoader;
    friend class ColumnReadStream;
    friend class DMFilePackFilter;
    friend class DMFileBlockInputStreamBuilder;
    friend class DMFileWithVectorIndexBlockInputStream;
    friend class tests::DMFileTest;
    friend class tests::DMFileMetaV2Test;
    friend class tests::DMStoreForSegmentReadTaskTest;
    friend int ::DTTool::Migrate::migrateServiceMain(
        DB::Context & context,
        const ::DTTool::Migrate::MigrateArgs & args);
    friend bool ::DTTool::Migrate::isRecognizable(const DB::DM::DMFile & file, const std::string & target);
    friend bool ::DTTool::Migrate::needFrameMigration(const DB::DM::DMFile & file, const std::string & target);
};

} // namespace DB::DM
