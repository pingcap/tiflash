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

#include <Common/FailPoint.h>
#include <IO/FileProvider/FileProvider.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <aws/s3/model/CommonPrefix.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <filesystem>
#include <utility>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_ALL_DATA;
extern const int INCORRECT_DATA;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_before_dmfile_remove_encryption[];
extern const char exception_before_dmfile_remove_from_disk[];
extern const char force_use_dmfile_format_v3[];
} // namespace FailPoints

namespace DM
{

String DMFile::path() const
{
    return getPathByStatus(parentPath(), fileId(), getStatus());
}

String DMFile::ngcPath() const
{
    return getNGCPath(parentPath(), fileId(), getStatus());
}

DMFilePtr DMFile::create(
    UInt64 file_id,
    const String & parent_path,
    DMConfigurationOpt configuration,
    UInt64 small_file_size_threshold,
    UInt64 merged_file_max_size,
    KeyspaceID keyspace_id,
    DMFileFormat::Version version)
{
    // if small_file_size_threshold == 0 we should use DMFileFormat::V2
    if (version == DMFileFormat::V3 and small_file_size_threshold == 0)
    {
        version = DMFileFormat::V2;
    }

    fiu_do_on(FailPoints::force_use_dmfile_format_v3, {
        // some unit test we need mock upload DMFile to S3, which only support DMFileFormat::V3
        version = DMFileFormat::V3;
    });
    // On create, ref_id is the same as file_id.
    DMFilePtr new_dmfile(new DMFile(
        file_id,
        file_id,
        parent_path,
        DMFileStatus::WRITABLE,
        small_file_size_threshold,
        merged_file_max_size,
        std::move(configuration),
        version,
        keyspace_id));

    auto path = new_dmfile->path();
    Poco::File file(path);
    if (file.exists())
    {
        file.remove(true);
        LOG_WARNING(Logger::get(), "Existing dmfile, removed: {}", path);
    }

    file.createDirectories();
    // Create a mark file to stop this dmfile from being removed by GC.
    // We should create NGC file after creating the directory under folder mode
    // since the NGC file is a file under the folder.
    // FIXME : this should not use PageUtils.
    PageUtil::touchFile(new_dmfile->ngcPath());

    return new_dmfile;
}

DMFilePtr DMFile::restore(
    const FileProviderPtr & file_provider,
    UInt64 file_id,
    UInt64 page_id,
    const String & parent_path,
    const DMFileMeta::ReadMode & read_meta_mode,
    KeyspaceID keyspace_id)
{
    auto is_s3_file = S3::S3FilenameView::fromKeyWithPrefix(parent_path).isDataFile();
    if (!is_s3_file)
    {
        // Unrecognized xx:// protocol.
        RUNTIME_CHECK_MSG(parent_path.find("://") == std::string::npos, "Unsupported protocol in path {}", parent_path);
        String path = getPathByStatus(parent_path, file_id, DMFileStatus::READABLE);
        // The path may be dropped by another thread in some cases
        auto poco_file = Poco::File(path);
        if (!poco_file.exists())
            return nullptr;
    }

    DMFilePtr dmfile(new DMFile(
        file_id,
        page_id,
        parent_path,
        DMFileStatus::READABLE,
        /*small_file_size_threshold_*/ 128 * 1024,
        /*merged_file_max_size_*/ 16 * 1024 * 1024,
        /*configuration_*/ std::nullopt,
        /*version_*/ STORAGE_FORMAT_CURRENT.dm_file,
        /*keyspace_id_*/ keyspace_id));
    if (is_s3_file || Poco::File(dmfile->metav2Path()).exists())
    {
        dmfile->meta = std::make_unique<DMFileMetaV2>(
            file_id,
            parent_path,
            DMFileStatus::READABLE,
            128 * 1024,
            16 * 1024 * 1024,
            keyspace_id,
            std::nullopt,
            STORAGE_FORMAT_CURRENT.dm_file);
        dmfile->meta->read(file_provider, read_meta_mode);
    }
    else if (!read_meta_mode.isNone())
    {
        dmfile->meta = std::make_unique<DMFileMeta>(
            file_id,
            parent_path,
            DMFileStatus::READABLE,
            keyspace_id,
            std::nullopt,
            STORAGE_FORMAT_CURRENT.dm_file);
        dmfile->meta->read(file_provider, read_meta_mode);
    }
    return dmfile;
}

String DMFile::colIndexCacheKey(const FileNameBase & file_name_base) const
{
    return colIndexPath(file_name_base);
}

String DMFile::colMarkCacheKey(const FileNameBase & file_name_base) const
{
    return colMarkPath(file_name_base);
}

bool DMFile::isColIndexExist(const ColId & col_id) const
{
    if (useMetaV2())
    {
        auto itr = meta->column_stats.find(col_id);
        return itr != meta->column_stats.end() && itr->second.index_bytes > 0;
    }
    else
    {
        return meta->column_indices.contains(col_id);
    }
}

size_t DMFile::colIndexSize(ColId id)
{
    if (useMetaV2())
    {
        if (auto itr = meta->column_stats.find(id); itr != meta->column_stats.end() && itr->second.index_bytes > 0)
        {
            return itr->second.index_bytes;
        }
        else
        {
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Index of {} not exist", id);
        }
    }
    else
    {
        // Even metav2 is not enabled, we can still read the index size from column_stats instead of disk IO?
        return colIndexSizeByName(getFileNameBase(id));
    }
}

// Only used when metav2 is not enabled, clean it up
size_t DMFile::colDataSize(ColId id, ColDataType type)
{
    if (useMetaV2())
    {
        auto itr = meta->column_stats.find(id);
        RUNTIME_CHECK_MSG(itr != meta->column_stats.end(), "Data of column not exist, col_id={} path={}", id, path());
        switch (type)
        {
        case ColDataType::Elements:
            return itr->second.data_bytes;
        case ColDataType::NullMap:
            return itr->second.nullmap_data_bytes;
        case ColDataType::ArraySizes:
            return itr->second.array_sizes_bytes;
        }
    }
    else
    {
        String namebase;
        switch (type)
        {
        case ColDataType::Elements:
            namebase = getFileNameBase(id);
            break;
        case ColDataType::NullMap:
            namebase = getFileNameBase(id, {IDataType::Substream::NullMap});
            break;
        case ColDataType::ArraySizes:
            RUNTIME_CHECK_MSG(
                type != ColDataType::ArraySizes,
                "Can not get array map size by filename, col_id={} path={}",
                id,
                path());
            break;
        }
        return colDataSizeByName(namebase);
    }
}

String DMFile::encryptionBasePath() const
{
    return getPathByStatus(parentPath(), fileId(), DMFileStatus::READABLE);
}

EncryptionPath DMFile::encryptionDataPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), file_name_base + details::DATA_FILE_SUFFIX, keyspaceId());
}

EncryptionPath DMFile::encryptionIndexPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), file_name_base + details::INDEX_FILE_SUFFIX, keyspaceId());
}

EncryptionPath DMFile::encryptionMarkPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), file_name_base + details::MARK_FILE_SUFFIX, keyspaceId());
}

void DMFile::finalize()
{
    if (STORAGE_FORMAT_CURRENT.dm_file >= DMFileFormat::V2 && !meta->configuration)
    {
        LOG_WARNING(log, "checksum disabled due to lack of configuration");
    }
    RUNTIME_CHECK_MSG(
        getStatus() == DMFileStatus::WRITING,
        "FileId={} Expected WRITING status, but {}",
        fileId(),
        magic_enum::enum_name(getStatus()));
    Poco::File old_file(path());
    setStatus(DMFileStatus::READABLE);

    auto new_path = path();
    Poco::File file(new_path);
    if (file.exists())
    {
        LOG_WARNING(log, "Existing dmfile, removing: {}", new_path);
        const String deleted_path = getPathByStatus(parentPath(), fileId(), DMFileStatus::DROPPED);
        // no need to delete the encryption info associated with the dmfile path here.
        // because this dmfile path is still a valid path and no obsolete encryption info will be left.
        file.renameTo(deleted_path);
        file.remove(true);
        LOG_WARNING(log, "Existing dmfile, removed: {}", deleted_path);
    }
    old_file.renameTo(new_path);
    // MetaV2 column index may be merged into merged file, so column_indices is unused.
    if (auto * metav1 = typeid_cast<DMFileMeta *>(meta.get()); metav1)
        meta->initializeIndices();
}

std::vector<String> DMFile::listLocal(const String & parent_path)
{
    Poco::File folder(parent_path);
    std::vector<String> file_names;
    if (folder.exists())
    {
        folder.list(file_names);
    }
    return file_names;
}

std::vector<String> DMFile::listS3(const String & parent_path)
{
    std::vector<String> filenames;
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    auto list_prefix = parent_path + "/";
    S3::listPrefixWithDelimiter(
        *client,
        list_prefix,
        /*delimiter*/ "/",
        [&filenames, &list_prefix](const Aws::S3::Model::CommonPrefix & prefix) {
            RUNTIME_CHECK(prefix.GetPrefix().size() > list_prefix.size(), prefix.GetPrefix(), list_prefix);
            auto short_name_size = prefix.GetPrefix().size() - list_prefix.size() - 1; // `1` for the delimiter in last.
            filenames.push_back(
                prefix.GetPrefix().substr(list_prefix.size(), short_name_size)); // Cut prefix and last delimiter.
            return S3::PageResult{.num_keys = 1, .more = true};
        });
    return filenames;
}

std::set<UInt64> DMFile::listAllInPath(
    const FileProviderPtr & file_provider,
    const String & parent_path,
    const DMFile::ListOptions & options,
    KeyspaceID keyspace_id)
{
    auto s3_fname_view = S3::S3FilenameView::fromKeyWithPrefix(parent_path);
    auto file_names = s3_fname_view.isValid() ? listS3(s3_fname_view.toFullKey()) : listLocal(parent_path);

    std::set<UInt64> file_ids;
    auto try_parse_file_id = [](const String & name) -> std::optional<UInt64> {
        std::vector<std::string> ss;
        boost::split(ss, name, boost::is_any_of("_"));
        if (ss.size() != 2)
            return std::nullopt;
        size_t pos;
        auto id = std::stoull(ss[1], &pos);
        // pos < ss[1].size() means that ss[1] is not an invalid integer
        return pos < ss[1].size() ? std::nullopt : std::make_optional(id);
    };

    for (const auto & name : file_names)
    {
        // Clean up temporary files and files should be deleted
        // Note that you should not do clean up if some DTFiles are writing,
        // or you may delete some writing files
        if (options.clean_up)
        {
            if (startsWith(name, details::FOLDER_PREFIX_WRITABLE) || startsWith(name, details::FOLDER_PREFIX_DROPPED))
            {
                // Clear temporary/deleted (maybe broken) files
                // The encryption info use readable path. We are not sure the encryption info is deleted or not.
                // Try to delete and ignore if it is already deleted.
                auto res = try_parse_file_id(name);
                if (!res)
                {
                    LOG_INFO(Logger::get(), "Unrecognized temporary or dropped dmfile, ignored: {}", name);
                    continue;
                }
                UInt64 file_id = *res;
                const String readable_path = getPathByStatus(parent_path, file_id, DMFileStatus::READABLE);
                file_provider->deleteEncryptionInfo(
                    EncryptionPath(readable_path, "", keyspace_id),
                    /* throw_on_error= */ false);
                const auto full_path = parent_path + "/" + name;
                if (Poco::File file(full_path); file.exists())
                    file.remove(true);
                LOG_WARNING(Logger::get(), "Existing temporary or dropped dmfile, removed: {}", full_path);
                continue;
            }
        }

        if (!startsWith(name, details::FOLDER_PREFIX_READABLE))
            continue;
        // When single_file_mode is enabled, ngc file will appear in the same level of directory with dmfile. Just ignore it.
        if (endsWith(name, details::NGC_FILE_NAME))
            continue;
        auto res = try_parse_file_id(name);
        if (!res)
        {
            LOG_INFO(Logger::get(), "Unrecognized DM file, ignored: {}", name);
            continue;
        }
        UInt64 file_id = *res;

        if (options.only_list_can_gc)
        {
            // Only return the ID if the file is able to be GC-ed.
            const auto file_path = parent_path + "/" + name;
            Poco::File file(file_path);
            String ngc_path = details::getNGCPath(file_path);
            Poco::File ngc_file(ngc_path);
            if (!ngc_file.exists())
                file_ids.insert(file_id);
        }
        else
        {
            file_ids.insert(file_id);
        }
    }
    return file_ids;
}

bool DMFile::canGC() const
{
    return !Poco::File(ngcPath()).exists();
}

void DMFile::enableGC() const
{
    Poco::File ngc_file(ngcPath());
    if (ngc_file.exists())
        ngc_file.remove();
}

void DMFile::remove(const FileProviderPtr & file_provider)
{
    // If we use `FileProvider::deleteDirectory`, it may left a broken DMFile on disk.
    // By renaming DMFile with a prefix first, even if there are broken DMFiles left,
    // we can safely clean them when `DMFile::listAllInPath` is called.
    const String dir_path = path();
    if (Poco::File dir_file(dir_path); dir_file.exists())
    {
        setStatus(DMFileStatus::DROPPED);
        const String deleted_path = path();
        // Rename the directory first (note that we should do it before deleting encryption info)
        dir_file.renameTo(deleted_path);
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_encryption);
        file_provider->deleteEncryptionInfo(EncryptionPath(encryptionBasePath(), "", keyspaceId()));
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_from_disk);
        // Then clean the files on disk
        dir_file.remove(true);
    }
}

std::vector<String> DMFile::listFilesForUpload() const
{
    RUNTIME_CHECK(useMetaV2());
    std::vector<String> fnames;
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator itr(path()); itr != end; ++itr)
    {
        if (itr.name() != "NGC")
        {
            fnames.emplace_back(itr.name());
        }
    }
    return fnames;
}

void DMFile::switchToRemote(const S3::DMFileOID & oid)
{
    RUNTIME_CHECK(useMetaV2());
    RUNTIME_CHECK(getStatus() == DMFileStatus::READABLE);

    auto local_path = path();
    // Update the parent_path so that it will read data from remote storage.
    meta->parent_path = S3::S3Filename::fromTableID(oid.store_id, oid.keyspace_id, oid.table_id).toFullKeyWithPrefix();

    // Remove local directory.
    std::filesystem::remove_all(local_path);
}

} // namespace DM
} // namespace DB
