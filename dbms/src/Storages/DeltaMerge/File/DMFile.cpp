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
#include <Common/StringUtils/StringRefUtils.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/IOSWrapper.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <aws/s3/model/CommonPrefix.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <atomic>
#include <boost/algorithm/string/classification.hpp>
#include <filesystem>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_ALL_DATA;
extern const int CORRUPTED_DATA;
extern const int INCORRECT_DATA;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_before_dmfile_remove_encryption[];
extern const char exception_before_dmfile_remove_from_disk[];
extern const char force_use_dmfile_format_v3[];
} // namespace FailPoints

namespace DM
{
namespace details
{
inline constexpr static const char * NGC_FILE_NAME = "NGC";
inline constexpr static const char * FOLDER_PREFIX_WRITABLE = ".tmp.dmf_";
inline constexpr static const char * FOLDER_PREFIX_READABLE = "dmf_";
inline constexpr static const char * FOLDER_PREFIX_DROPPED = ".del.dmf_";
inline constexpr static const char * DATA_FILE_SUFFIX = ".dat";
inline constexpr static const char * INDEX_FILE_SUFFIX = ".idx";
inline constexpr static const char * MARK_FILE_SUFFIX = ".mrk";

inline String getNGCPath(const String & prefix)
{
    return prefix + "/" + NGC_FILE_NAME;
}
} // namespace details

// Some static helper functions

String DMFile::getPathByStatus(const String & parent_path, UInt64 file_id, DMFile::Status status)
{
    String s = parent_path + "/";
    switch (status)
    {
    case DMFile::Status::READABLE:
        s += details::FOLDER_PREFIX_READABLE;
        break;
    case DMFile::Status::WRITABLE:
    case DMFile::Status::WRITING:
        s += details::FOLDER_PREFIX_WRITABLE;
        break;
    case DMFile::Status::DROPPED:
        s += details::FOLDER_PREFIX_DROPPED;
        break;
    }
    s += DB::toString(file_id);
    return s;
}

String DMFile::getNGCPath(const String & parent_path, UInt64 file_id, DMFile::Status status)
{
    return details::getNGCPath(getPathByStatus(parent_path, file_id, status));
}

//

String DMFile::path() const
{
    return getPathByStatus(parent_path, file_id, status);
}

String DMFile::ngcPath() const
{
    return getNGCPath(parent_path, file_id, status);
}

DMFilePtr DMFile::create(
    UInt64 file_id,
    const String & parent_path,
    DMConfigurationOpt configuration,
    UInt64 small_file_size_threshold,
    UInt64 merged_file_max_size,
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
        LOG_WARNING(Logger::get(), "!!!force use DMFileFormat::V3!!!");
    });
    // On create, ref_id is the same as file_id.
    DMFilePtr new_dmfile(new DMFile(
        file_id,
        file_id,
        parent_path,
        Status::WRITABLE,
        small_file_size_threshold,
        merged_file_max_size,
        std::move(configuration),
        version));

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
    const ReadMetaMode & read_meta_mode)
{
    auto is_s3_file = S3::S3FilenameView::fromKeyWithPrefix(parent_path).isDataFile();
    if (!is_s3_file)
    {
        // Unrecognized xx:// protocol.
        RUNTIME_CHECK_MSG(parent_path.find("://") == std::string::npos, "Unsupported protocol in path {}", parent_path);
        String path = getPathByStatus(parent_path, file_id, DMFile::Status::READABLE);
        // The path may be dropped by another thread in some cases
        auto poco_file = Poco::File(path);
        if (!poco_file.exists())
            return nullptr;
    }

    DMFilePtr dmfile(new DMFile(file_id, page_id, parent_path, Status::READABLE));
    if (is_s3_file || Poco::File(dmfile->metav2Path()).exists())
    {
        auto s = dmfile->readMetaV2(file_provider);
        dmfile->parseMetaV2(std::string_view(s.data(), s.size()));
    }
    else if (!read_meta_mode.isNone())
    {
        dmfile->readConfiguration(file_provider);
        dmfile->readMetadata(file_provider, read_meta_mode);
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
        auto itr = column_stats.find(col_id);
        return itr != column_stats.end() && itr->second.index_bytes > 0;
    }
    else
    {
        return column_indices.count(col_id) != 0;
    }
}

size_t DMFile::colIndexSize(ColId id)
{
    if (useMetaV2())
    {
        if (auto itr = column_stats.find(id); itr != column_stats.end() && itr->second.index_bytes > 0)
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
        return colIndexSizeByName(getFileNameBase(id));
    }
}

// Only used when metav2 is not enabled, clean it up
size_t DMFile::colDataSize(ColId id, ColDataType type)
{
    if (useMetaV2())
    {
        auto itr = column_stats.find(id);
        RUNTIME_CHECK_MSG(itr != column_stats.end(), "Data of column not exist, col_id={} path={}", id, path());
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
    return getPathByStatus(parent_path, file_id, DMFile::Status::READABLE);
}


EncryptionPath DMFile::encryptionDataPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), file_name_base + details::DATA_FILE_SUFFIX);
}

EncryptionPath DMFile::encryptionIndexPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), file_name_base + details::INDEX_FILE_SUFFIX);
}

EncryptionPath DMFile::encryptionMarkPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), file_name_base + details::MARK_FILE_SUFFIX);
}

EncryptionPath DMFile::encryptionMetaPath() const
{
    return EncryptionPath(encryptionBasePath(), metaFileName());
}

EncryptionPath DMFile::encryptionPackStatPath() const
{
    return EncryptionPath(encryptionBasePath(), packStatFileName());
}

EncryptionPath DMFile::encryptionPackPropertyPath() const
{
    return EncryptionPath(encryptionBasePath(), packPropertyFileName());
}

EncryptionPath DMFile::encryptionConfigurationPath() const
{
    return EncryptionPath(encryptionBasePath(), configurationFileName());
}

EncryptionPath DMFile::encryptionMetav2Path() const
{
    return EncryptionPath(encryptionBasePath(), metav2FileName());
}

EncryptionPath DMFile::encryptionMergedPath(UInt32 number) const
{
    return EncryptionPath(encryptionBasePath(), mergedFilename(number));
}

String DMFile::colDataFileName(const FileNameBase & file_name_base)
{
    return file_name_base + details::DATA_FILE_SUFFIX;
}
String DMFile::colIndexFileName(const FileNameBase & file_name_base)
{
    return file_name_base + details::INDEX_FILE_SUFFIX;
}
String DMFile::colMarkFileName(const FileNameBase & file_name_base)
{
    return file_name_base + details::MARK_FILE_SUFFIX;
}

DMFile::OffsetAndSize DMFile::writeMetaToBuffer(WriteBuffer & buffer)
{
    size_t meta_offset = buffer.count();
    writeString("DTFile format: ", buffer);
    writeIntText(configuration ? DMFileFormat::V2 : DMFileFormat::V1, buffer);
    writeString("\n", buffer);
    writeText(column_stats, STORAGE_FORMAT_CURRENT.dm_file, buffer);
    size_t meta_size = buffer.count() - meta_offset;
    return std::make_tuple(meta_offset, meta_size);
}

DMFile::OffsetAndSize DMFile::writePackStatToBuffer(WriteBuffer & buffer)
{
    size_t pack_offset = buffer.count();
    for (auto & stat : pack_stats)
    {
        writePODBinary(stat, buffer);
    }
    size_t pack_size = buffer.count() - pack_offset;
    return std::make_tuple(pack_offset, pack_size);
}

DMFile::OffsetAndSize DMFile::writePackPropertyToBuffer(WriteBuffer & buffer, UnifiedDigestBase * digest)
{
    size_t offset = buffer.count();
    auto data = pack_properties.SerializeAsString();
    if (digest)
    {
        digest->update(data.data(), data.size());
    }
    writeStringBinary(data, buffer);
    size_t size = buffer.count() - offset;
    return std::make_tuple(offset, size);
}

void DMFile::writeMeta(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    String meta_path = metaPath();
    String tmp_meta_path = meta_path + ".tmp";

    {
        WriteBufferFromFileProvider buf(file_provider, tmp_meta_path, encryptionMetaPath(), false, write_limiter, 4096);
        if (configuration)
        {
            auto digest = configuration->createUnifiedDigest();
            auto tmp_buffer = WriteBufferFromOwnString{};
            writeMetaToBuffer(tmp_buffer);
            auto serialized = tmp_buffer.releaseStr();
            digest->update(serialized.data(), serialized.length());
            configuration->addChecksum(metaFileName(), digest->raw());
            buf.write(serialized.data(), serialized.size());
        }
        else
        {
            writeMetaToBuffer(buf);
        }
        buf.sync();
    }
    Poco::File(tmp_meta_path).renameTo(meta_path);
}

void DMFile::writePackProperty(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    String property_path = packPropertyPath();
    String tmp_property_path = property_path + ".tmp";
    {
        WriteBufferFromFileProvider
            buf(file_provider, tmp_property_path, encryptionPackPropertyPath(), false, write_limiter, 4096);
        if (configuration)
        {
            auto digest = configuration->createUnifiedDigest();
            writePackPropertyToBuffer(buf, digest.get());
            configuration->addChecksum(packPropertyFileName(), digest->raw());
        }
        else
        {
            writePackPropertyToBuffer(buf);
        }
        buf.sync();
    }
    Poco::File(tmp_property_path).renameTo(property_path);
}


void DMFile::writeConfiguration(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    assert(configuration);
    String config_path = configurationPath();
    String tmp_config_path = config_path + ".tmp";
    {
        WriteBufferFromFileProvider buf(
            file_provider,
            tmp_config_path,
            encryptionConfigurationPath(),
            false,
            write_limiter,
            DBMS_DEFAULT_BUFFER_SIZE);
        {
            auto stream = OutputStreamWrapper{buf};
            stream << *configuration;
        }
        buf.sync();
    }
    Poco::File(tmp_config_path).renameTo(config_path);
}

void DMFile::writeMetadata(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    writePackProperty(file_provider, write_limiter);
    writeMeta(file_provider, write_limiter);
    if (configuration)
    {
        writeConfiguration(file_provider, write_limiter);
    }
}

void DMFile::upgradeMetaIfNeed(const FileProviderPtr & file_provider, DMFileFormat::Version ver)
{
    if (unlikely(ver == DMFileFormat::V0))
    {
        // Update ColumnStat.serialized_bytes
        for (auto && c : column_stats)
        {
            auto col_id = c.first;
            auto & stat = c.second;
            c.second.type->enumerateStreams(
                [col_id, &stat, this](const IDataType::SubstreamPath & substream) {
                    String stream_name = DMFile::getFileNameBase(col_id, substream);
                    String data_file = colDataPath(stream_name);
                    if (Poco::File f(data_file); f.exists())
                        stat.serialized_bytes += f.getSize();
                    String mark_file = colDataPath(stream_name);
                    if (Poco::File f(mark_file); f.exists())
                        stat.serialized_bytes += f.getSize();
                    String index_file = colIndexPath(stream_name);
                    if (Poco::File f(index_file); f.exists())
                        stat.serialized_bytes += f.getSize();
                },
                {});
        }
        // Update ColumnStat in meta.
        writeMeta(file_provider, nullptr);
    }
}

void DMFile::readColumnStat(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info)
{
    const auto name = metaFileName();
    auto file_buf = openForRead(file_provider, metaPath(), encryptionMetaPath(), meta_pack_info.column_stat_size);
    auto meta_buf = std::vector<char>(meta_pack_info.column_stat_size);
    auto meta_reader = ReadBufferFromMemory{meta_buf.data(), meta_buf.size()};
    ReadBuffer * buf = &file_buf;
    file_buf.seek(meta_pack_info.column_stat_offset);

    // checksum examination
    if (configuration)
    {
        auto location = configuration->getEmbeddedChecksum().find(name);
        if (location != configuration->getEmbeddedChecksum().end())
        {
            auto digest = configuration->createUnifiedDigest();
            file_buf.readBig(meta_buf.data(), meta_buf.size());
            digest->update(meta_buf.data(), meta_buf.size());
            if (unlikely(!digest->compareRaw(location->second)))
            {
                throw TiFlashException(
                    fmt::format("checksum mismatch for {}", metaPath()),
                    Errors::Checksum::DataCorruption);
            }
            buf = &meta_reader;
        }
        else
        {
            LOG_WARNING(log, "checksum for {} not found", name);
        }
    }

    DMFileFormat::Version ver; // Binary version
    assertString("DTFile format: ", *buf);
    DB::readText(ver, *buf);
    assertString("\n", *buf);
    readText(column_stats, ver, *buf);

    // for V2, we do not apply in-place upgrade for now
    // but it should not affect the normal read procedure
    if (unlikely(ver >= DMFileFormat::V2 && !configuration))
    {
        throw TiFlashException("configuration expected but not loaded", Errors::Checksum::Missing);
    }
    upgradeMetaIfNeed(file_provider, ver);
}

void DMFile::readPackStat(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info)
{
    size_t packs = meta_pack_info.pack_stat_size / sizeof(PackStat);
    pack_stats.resize(packs);
    const auto path = packStatPath();
    if (configuration)
    {
        auto buf = createReadBufferFromFileBaseByFileProvider(
            file_provider,
            path,
            encryptionPackStatPath(),
            configuration->getChecksumFrameLength(),
            nullptr,
            configuration->getChecksumAlgorithm(),
            configuration->getChecksumFrameLength());
        buf->seek(meta_pack_info.pack_stat_offset);
        if (sizeof(PackStat) * packs
            != buf->readBig(reinterpret_cast<char *>(pack_stats.data()), sizeof(PackStat) * packs))
        {
            throw Exception("Cannot read all data", ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
    else
    {
        auto buf = openForRead(file_provider, path, encryptionPackStatPath(), meta_pack_info.pack_stat_size);
        buf.seek(meta_pack_info.pack_stat_offset);
        if (sizeof(PackStat) * packs
            != buf.readBig(reinterpret_cast<char *>(pack_stats.data()), sizeof(PackStat) * packs))
        {
            throw Exception("Cannot read all data", ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
}

void DMFile::readConfiguration(const FileProviderPtr & file_provider)
{
    if (Poco::File(configurationPath()).exists())
    {
        auto file
            = openForRead(file_provider, configurationPath(), encryptionConfigurationPath(), DBMS_DEFAULT_BUFFER_SIZE);
        auto stream = InputStreamWrapper{file};
        configuration.emplace(stream);
        version = DMFileFormat::V2;
    }
    else
    {
        configuration.reset();
        version = DMFileFormat::V1;
    }
}

void DMFile::readPackProperty(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info)
{
    String tmp_buf;
    const auto name = packPropertyFileName();
    auto buf = openForRead(
        file_provider,
        packPropertyPath(),
        encryptionPackPropertyPath(),
        meta_pack_info.pack_property_size);
    buf.seek(meta_pack_info.pack_property_offset);

    readStringBinary(tmp_buf, buf);
    pack_properties.ParseFromString(tmp_buf);

    if (configuration)
    {
        auto location = configuration->getEmbeddedChecksum().find(name);
        if (location != configuration->getEmbeddedChecksum().end())
        {
            auto digest = configuration->createUnifiedDigest();
            const auto & target = location->second;
            digest->update(tmp_buf.data(), tmp_buf.size());
            if (unlikely(!digest->compareRaw(target)))
            {
                throw TiFlashException(
                    fmt::format("checksum mismatch for {}", packPropertyPath()),
                    Errors::Checksum::DataCorruption);
            }
        }
        else
        {
            LOG_WARNING(log, "checksum for {} not found", name);
        }
    }
}

// Only used when metav2 is not enabled
void DMFile::readMetadata(const FileProviderPtr & file_provider, const ReadMetaMode & read_meta_mode)
{
    Footer footer;

    if (read_meta_mode.isAll())
    {
        initializeIndices();
    }
    if (auto file = Poco::File(packPropertyPath()); file.exists())
        footer.meta_pack_info.pack_property_size = file.getSize();

    auto recheck = [&](size_t size) {
        if (this->configuration)
        {
            auto total_size
                = this->configuration->getChecksumFrameLength() + this->configuration->getChecksumHeaderLength();
            auto frame_count = size / total_size + (0 != size % total_size);
            size -= frame_count * this->configuration->getChecksumHeaderLength();
        }
        return size;
    };

    if (auto file = Poco::File(packPropertyPath()); file.exists())
        footer.meta_pack_info.pack_property_size = file.getSize();

    footer.meta_pack_info.column_stat_size = Poco::File(metaPath()).getSize();
    footer.meta_pack_info.pack_stat_size = recheck(Poco::File(packStatPath()).getSize());

    if (read_meta_mode.needPackProperty() && footer.meta_pack_info.pack_property_size != 0)
        readPackProperty(file_provider, footer.meta_pack_info);

    if (read_meta_mode.needColumnStat())
        readColumnStat(file_provider, footer.meta_pack_info);

    if (read_meta_mode.needPackStat())
        readPackStat(file_provider, footer.meta_pack_info);
}

void DMFile::finalizeForFolderMode(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    if (STORAGE_FORMAT_CURRENT.dm_file >= DMFileFormat::V2 && !configuration)
    {
        LOG_WARNING(log, "checksum disabled due to lack of configuration");
    }
    writeMetadata(file_provider, write_limiter);
    if (unlikely(status != Status::WRITING))
        throw Exception("Expected WRITING status, now " + statusString(status));
    Poco::File old_file(path());
    setStatus(Status::READABLE);

    auto new_path = path();

    Poco::File file(new_path);
    if (file.exists())
    {
        LOG_WARNING(log, "Existing dmfile, removing: {}", new_path);
        const String deleted_path = getPathByStatus(parent_path, file_id, Status::DROPPED);
        // no need to delete the encryption info associated with the dmfile path here.
        // because this dmfile path is still a valid path and no obsolete encryption info will be left.
        file.renameTo(deleted_path);
        file.remove(true);
        LOG_WARNING(log, "Existing dmfile, removed: {}", deleted_path);
    }
    old_file.renameTo(new_path);
    initializeIndices();
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
    const DMFile::ListOptions & options)
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
                const String readable_path = getPathByStatus(parent_path, file_id, DMFile::Status::READABLE);
                file_provider->deleteEncryptionInfo(EncryptionPath(readable_path, ""), /* throw_on_error= */ false);
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
        setStatus(Status::DROPPED);
        const String deleted_path = path();
        // Rename the directory first (note that we should do it before deleting encryption info)
        dir_file.renameTo(deleted_path);
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_encryption);
        file_provider->deleteEncryptionInfo(EncryptionPath(encryptionBasePath(), ""));
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_from_disk);
        // Then clean the files on disk
        dir_file.remove(true);
    }
}

void DMFile::initializeIndices()
{
    auto decode = [](const StringRef & data) {
        try
        {
            auto original = unescapeForFileName(data);
            return std::stoll(original);
        }
        catch (const std::invalid_argument & err)
        {
            throw DB::Exception(fmt::format("invalid ColId: {} from file: {}", err.what(), data));
        }
        catch (const std::out_of_range & err)
        {
            throw DB::Exception(fmt::format("invalid ColId: {} from file: {}", err.what(), data));
        }
    };

    Poco::File directory{path()};
    std::vector<std::string> sub_files{};
    directory.list(sub_files);
    for (const auto & name : sub_files)
    {
        if (endsWith(name, details::INDEX_FILE_SUFFIX))
        {
            column_indices.insert(
                decode(removeSuffix(name, strlen(details::INDEX_FILE_SUFFIX)))); // strip tailing `.idx`
        }
    }
}

DMFile::MetaBlockHandle DMFile::writeSLPackStatToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();
    const char * data = reinterpret_cast<const char *>(&pack_stats[0]);
    size_t size = pack_stats.size() * sizeof(PackStat);
    writeString(data, size, buffer);
    return MetaBlockHandle{MetaBlockType::PackStat, offset, buffer.count() - offset};
}

DMFile::MetaBlockHandle DMFile::writeSLPackPropertyToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();
    for (const auto & pb : pack_properties.property())
    {
        PackProperty tmp{pb};
        const char * data = reinterpret_cast<const char *>(&tmp);
        size_t size = sizeof(PackProperty);
        writeString(data, size, buffer);
    }
    return MetaBlockHandle{MetaBlockType::PackProperty, offset, buffer.count() - offset};
}

DMFile::MetaBlockHandle DMFile::writeColumnStatToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();
    writeIntBinary(column_stats.size(), buffer);
    for (const auto & [id, stat] : column_stats)
    {
        stat.serializeToBuffer(buffer);
    }
    return MetaBlockHandle{MetaBlockType::ColumnStat, offset, buffer.count() - offset};
}

DMFile::MetaBlockHandle DMFile::writeExtendColumnStatToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();
    dtpb::ColumnStats msg_stats;
    for (const auto & [id, stat] : column_stats)
    {
        auto msg = stat.toProto();
        msg_stats.add_column_stats()->Swap(&msg);
    }
    String output;
    msg_stats.SerializeToString(&output);
    writeString(output.data(), output.length(), buffer);
    return MetaBlockHandle{MetaBlockType::ExtendColumnStat, offset, buffer.count() - offset};
}

DMFile::MetaBlockHandle DMFile::writeMergedSubFilePosotionsToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();

    writeIntBinary(merged_files.size(), buffer);
    const auto * data = reinterpret_cast<const char *>(&merged_files[0]);
    auto bytes = merged_files.size() * sizeof(MergedFile);
    writeString(data, bytes, buffer);

    writeIntBinary(merged_sub_file_infos.size(), buffer);
    for (const auto & [fname, info] : merged_sub_file_infos)
    {
        info.serializeToBuffer(buffer);
    }
    return MetaBlockHandle{MetaBlockType::MergedSubFilePos, offset, buffer.count() - offset};
}

void DMFile::finalizeMetaV2(WriteBuffer & buffer)
{
    auto tmp_buffer = WriteBufferFromOwnString{};
    std::array meta_block_handles = {
        //
        writeSLPackStatToBuffer(tmp_buffer),
        writeSLPackPropertyToBuffer(tmp_buffer),
        writeColumnStatToBuffer(tmp_buffer),
        writeExtendColumnStatToBuffer(tmp_buffer),
        writeMergedSubFilePosotionsToBuffer(tmp_buffer),
    };
    writePODBinary(meta_block_handles, tmp_buffer);
    writeIntBinary(static_cast<UInt64>(meta_block_handles.size()), tmp_buffer);
    writeIntBinary(version, tmp_buffer);

    // Write to file and do checksums.
    auto s = tmp_buffer.releaseStr();
    writeString(s.data(), s.size(), buffer);
    if (configuration)
    {
        auto digest = configuration->createUnifiedDigest();
        digest->update(s.data(), s.size());
        auto checksum_result = digest->raw();
        writeString(checksum_result.data(), checksum_result.size(), buffer);
    }

    MetaFooter footer{};
    if (configuration)
    {
        footer.checksum_algorithm = static_cast<UInt64>(configuration->getChecksumAlgorithm());
        footer.checksum_frame_length = configuration->getChecksumFrameLength();
    }
    writePODBinary(footer, buffer);
}

std::vector<char> DMFile::readMetaV2(const FileProviderPtr & file_provider)
{
    auto rbuf = openForRead(file_provider, metav2Path(), encryptionMetav2Path(), meta_buffer_size);
    std::vector<char> buf(meta_buffer_size);
    size_t read_bytes = 0;
    for (;;)
    {
        read_bytes += rbuf.readBig(buf.data() + read_bytes, meta_buffer_size);
        if (likely(read_bytes < buf.size()))
        {
            break;
        }
        LOG_WARNING(log, "{}'s size is larger than {}", metav2Path(), buf.size());
        buf.resize(buf.size() + meta_buffer_size);
    }
    buf.resize(read_bytes);
    return buf;
}

void DMFile::parseMetaV2(std::string_view buffer)
{
    // MetaFooter
    const auto * footer = reinterpret_cast<const MetaFooter *>(buffer.data() + buffer.size() - sizeof(MetaFooter));
    if (footer->checksum_algorithm != 0 && footer->checksum_frame_length != 0)
    {
        configuration = DMChecksumConfig{
            /*embedded_checksum*/ {},
            footer->checksum_frame_length,
            static_cast<ChecksumAlgo>(footer->checksum_algorithm)};
    }
    else
    {
        configuration.reset();
    }

    const auto * ptr = reinterpret_cast<const char *>(footer);

    // Checksum
    if (configuration)
    {
        auto digest = configuration->createUnifiedDigest();
        auto hash_size = digest->hashSize();
        ptr = ptr - hash_size;
        digest->update(buffer.data(), buffer.size() - sizeof(MetaFooter) - hash_size);
        if (unlikely(!digest->compareRaw(ptr)))
        {
            LOG_ERROR(log, "{} checksum invalid", metav2Path());
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{} checksum invalid", metav2Path());
        }
    }

    ptr = ptr - sizeof(DMFileFormat::Version);
    version = *(reinterpret_cast<const DMFileFormat::Version *>(ptr));

    ptr = ptr - sizeof(UInt64);
    auto meta_block_handle_count = *(reinterpret_cast<const UInt64 *>(ptr));

    for (UInt64 i = 0; i < meta_block_handle_count; ++i)
    {
        ptr = ptr - sizeof(MetaBlockHandle);
        const auto * handle = reinterpret_cast<const MetaBlockHandle *>(ptr);
        // omit the default branch. If there are unknown MetaBlock (after in-place downgrade), just ignore and throw away
        switch (handle->type)
        {
        case MetaBlockType::ColumnStat: // parse the `ColumnStat` from old version
            parseColumnStat(buffer.substr(handle->offset, handle->size));
            break;
        case MetaBlockType::ExtendColumnStat:
            parseExtendColumnStat(buffer.substr(handle->offset, handle->size));
            break;
        case MetaBlockType::PackProperty:
            parsePackProperty(buffer.substr(handle->offset, handle->size));
            break;
        case MetaBlockType::PackStat:
            parsePackStat(buffer.substr(handle->offset, handle->size));
            break;
        case MetaBlockType::MergedSubFilePos:
            parseMergedSubFilePos(buffer.substr(handle->offset, handle->size));
            break;
        }
    }
}

void DMFile::parseColumnStat(std::string_view buffer)
{
    ReadBufferFromString rbuf(buffer);
    size_t count;
    readIntBinary(count, rbuf);
    column_stats.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        ColumnStat stat;
        stat.parseFromBuffer(rbuf);
        // Do not overwrite the ColumnStat if already exist, it may
        // created by `ExteandColumnStat`
        column_stats.emplace(stat.col_id, std::move(stat));
    }
}

void DMFile::parseExtendColumnStat(std::string_view buffer)
{
    dtpb::ColumnStats msg_stats;
    auto parse_ok = msg_stats.ParseFromArray(buffer.begin(), buffer.size());
    RUNTIME_CHECK_MSG(parse_ok, "Parse extend column stat fail! filename={}", path());
    column_stats.reserve(msg_stats.column_stats_size());
    for (int i = 0; i < msg_stats.column_stats_size(); ++i)
    {
        const auto & msg = msg_stats.column_stats(i);
        ColumnStat stat;
        stat.mergeFromProto(msg);
        // replace the ColumnStat if exists
        if (auto [iter, inserted] = column_stats.emplace(stat.col_id, stat); unlikely(!inserted))
        {
            iter->second = stat;
        }
    }
}

void DMFile::parseMergedSubFilePos(std::string_view buffer)
{
    ReadBufferFromString rbuf(buffer);

    UInt64 merged_files_count;
    readIntBinary(merged_files_count, rbuf);
    merged_files.resize(merged_files_count);
    readString(reinterpret_cast<char *>(merged_files.data()), merged_files_count * sizeof(MergedFile), rbuf);

    UInt64 count;
    readIntBinary(count, rbuf);
    merged_sub_file_infos.reserve(count);
    for (UInt64 i = 0; i < count; ++i)
    {
        auto t = MergedSubFileInfo::parseFromBuffer(rbuf);
        auto fname = t.fname;
        merged_sub_file_infos.emplace(std::move(fname), std::move(t));
    }
}

void DMFile::parsePackProperty(std::string_view buffer)
{
    const auto * pp = reinterpret_cast<const PackProperty *>(buffer.data());
    auto count = buffer.size() / sizeof(PackProperty);
    pack_properties.mutable_property()->Reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        pp[i].toProtobuf(pack_properties.add_property());
    }
}

void DMFile::parsePackStat(std::string_view buffer)
{
    auto count = buffer.size() / sizeof(PackStat);
    pack_stats.resize(count);
    memcpy(reinterpret_cast<char *>(pack_stats.data()), buffer.data(), buffer.size());
}

void DMFile::finalizeDirName()
{
    RUNTIME_CHECK_MSG(
        status == Status::WRITING,
        "FileId={} Expected WRITING status, but {}",
        file_id,
        statusString(status));
    Poco::File old_file(path());
    setStatus(Status::READABLE);
    auto new_path = path();
    Poco::File file(new_path);
    if (file.exists())
    {
        LOG_WARNING(log, "Existing dmfile, removing: {}", new_path);
        const String deleted_path = getPathByStatus(parent_path, file_id, Status::DROPPED);
        // no need to delete the encryption info associated with the dmfile path here.
        // because this dmfile path is still a valid path and no obsolete encryption info will be left.
        file.renameTo(deleted_path);
        file.remove(true);
        LOG_WARNING(log, "Existing dmfile, removed: {}", deleted_path);
    }
    old_file.renameTo(new_path);
}

std::vector<String> DMFile::listFilesForUpload()
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
    RUNTIME_CHECK(status == Status::READABLE);

    auto local_path = path();
    // Update the parent_path so that it will read data from remote storage.
    parent_path = S3::S3Filename::fromTableID(oid.store_id, oid.keyspace_id, oid.table_id).toFullKeyWithPrefix();

    // Remove local directory.
    std::filesystem::remove_all(local_path);
}


void DMFile::checkMergedFile(
    MergedFileWriter & writer,
    FileProviderPtr & file_provider,
    WriteLimiterPtr & write_limiter)
{
    if (writer.file_info.size >= merged_file_max_size)
    {
        // finialize cur merged file
        writer.buffer->sync();
        merged_files.push_back(writer.file_info);
        auto cur_number = writer.file_info.number;

        // create a new merge file
        writer.file_info.number = cur_number + 1;
        writer.file_info.size = 0;
        writer.buffer.reset();

        writer.buffer = std::make_unique<WriteBufferFromFileProvider>(
            file_provider,
            mergedPath(writer.file_info.number),
            encryptionMergedPath(writer.file_info.number),
            /*create_new_encryption_info*/ false,
            write_limiter);
    }
}

// Merge the small files into a single file to avoid
// filesystem inodes exhausting
void DMFile::finalizeSmallFiles(
    MergedFileWriter & writer,
    FileProviderPtr & file_provider,
    WriteLimiterPtr & write_limiter)
{
    auto copy_file_to_cur = [&](const String & fname, UInt64 fsize) {
        checkMergedFile(writer, file_provider, write_limiter);

        auto read_file
            = openForRead(file_provider, subFilePath(fname), EncryptionPath(encryptionBasePath(), fname), fsize);
        std::vector<char> read_buf(fsize);
        auto read_size = read_file.readBig(read_buf.data(), read_buf.size());
        RUNTIME_CHECK(read_size == fsize, fname, read_size, fsize);

        writer.buffer->write(read_buf.data(), read_buf.size());
        merged_sub_file_infos.emplace(
            fname,
            MergedSubFileInfo(
                fname,
                writer.file_info.number,
                /*offset*/ writer.file_info.size,
                /*size*/ read_buf.size()));
        writer.file_info.size += read_buf.size();
    };

    std::vector<String> delete_file_name;
    for (const auto & [col_id, stat] : column_stats)
    {
        // check .data
        if (stat.data_bytes <= small_file_size_threshold)
        {
            auto fname = colDataFileName(getFileNameBase(col_id, {}));
            auto fsize = stat.data_bytes;
            copy_file_to_cur(fname, fsize);
            delete_file_name.emplace_back(std::move(fname));
        }

        // check .null.data
        if (stat.type->isNullable() && stat.nullmap_data_bytes <= small_file_size_threshold)
        {
            auto fname = colDataFileName(getFileNameBase(col_id, {IDataType::Substream::NullMap}));
            auto fsize = stat.nullmap_data_bytes;
            copy_file_to_cur(fname, fsize);
            delete_file_name.emplace_back(std::move(fname));
        }

        // check .size0.dat
        if (stat.array_sizes_bytes > 0 && stat.array_sizes_bytes <= small_file_size_threshold)
        {
            auto fname = colDataFileName(getFileNameBase(col_id, {IDataType::Substream::ArraySizes}));
            auto fsize = stat.array_sizes_bytes;
            copy_file_to_cur(fname, fsize);
            delete_file_name.emplace_back(std::move(fname));
        }
    }

    writer.buffer->sync();
    merged_files.push_back(writer.file_info);

    for (auto & fname : delete_file_name)
    {
        std::filesystem::remove(subFilePath(fname));
    }
}

UInt64 DMFile::getFileSize(ColId col_id, const String & filename) const
{
    auto itr = column_stats.find(col_id);
    RUNTIME_CHECK(itr != column_stats.end(), col_id);
    if (endsWith(filename, ".idx"))
    {
        return itr->second.index_bytes;
    }
    // Note that ".null.dat"/"null.mrk" must be check before ".dat"/".mrk"
    else if (endsWith(filename, ".null.dat"))
    {
        return itr->second.nullmap_data_bytes;
    }
    else if (endsWith(filename, ".null.mrk"))
    {
        return itr->second.nullmap_mark_bytes;
    }
    // Note that ".size0.dat"/".size0.mrk" must be check before ".dat"/".mrk"
    else if (endsWith(filename, ".size0.dat"))
    {
        return itr->second.array_sizes_bytes;
    }
    else if (endsWith(filename, ".size0.mrk"))
    {
        return itr->second.array_sizes_mark_bytes;
    }
    else if (endsWith(filename, ".dat"))
    {
        return itr->second.data_bytes;
    }
    else if (endsWith(filename, ".mrk"))
    {
        return itr->second.mark_bytes;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknow filename={} col_id={}", filename, col_id);
    }
}

UInt64 DMFile::getReadFileSize(ColId col_id, const String & filename) const
{
    auto itr = merged_sub_file_infos.find(filename);
    if (itr != merged_sub_file_infos.end())
    {
        return getMergedFileSizeOfColumn(itr->second);
    }
    else
    {
        return getFileSize(col_id, filename);
    }
}

UInt64 DMFile::getMergedFileSizeOfColumn(const MergedSubFileInfo & file_info) const
{
    // Get filesize of merged file.
    auto itr = std::find_if(merged_files.begin(), merged_files.end(), [&file_info](const auto & merged_file) {
        return merged_file.number == file_info.number;
    });
    RUNTIME_CHECK(itr != merged_files.end());
    return itr->size;
}
} // namespace DM
} // namespace DB
