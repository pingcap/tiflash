// Copyright 2022 PingCAP, Ltd.
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
#include <IO/IOSWrapper.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/Page/PageUtil.h>
#include <boost_wrapper/string_split.h>
#include <fmt/format.h>

#include <boost/algorithm/string/classification.hpp>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_ALL_DATA;
}

namespace FailPoints
{
extern const char exception_before_dmfile_remove_encryption[];
extern const char exception_before_dmfile_remove_from_disk[];
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

inline String getNGCPath(const String & prefix, bool is_single_mode)
{
    return prefix + (is_single_mode ? "." : "/") + NGC_FILE_NAME;
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

String DMFile::getNGCPath(const String & parent_path, UInt64 file_id, DMFile::Status status, bool is_single_mode)
{
    return details::getNGCPath(getPathByStatus(parent_path, file_id, status), is_single_mode);
}

//

String DMFile::path() const
{
    return getPathByStatus(parent_path, file_id, status);
}

String DMFile::ngcPath() const
{
    return getNGCPath(parent_path, file_id, status, isSingleFileMode());
}

DMFilePtr DMFile::create(UInt64 file_id, const String & parent_path, bool single_file_mode, DMConfigurationOpt configuration)
{
    Poco::Logger * log = &Poco::Logger::get("DMFile");
    // On create, ref_id is the same as file_id.
    DMFilePtr new_dmfile(new DMFile(file_id,
                                    file_id,
                                    parent_path,
                                    single_file_mode ? Mode::SINGLE_FILE : Mode::FOLDER,
                                    Status::WRITABLE,
                                    log,
                                    std::move(configuration)));

    auto path = new_dmfile->path();
    Poco::File file(path);
    if (file.exists())
    {
        file.remove(true);
        LOG_WARNING(log, "Existing dmfile, removed: {}", path);
    }
    if (single_file_mode)
    {
        Poco::File parent(parent_path);
        parent.createDirectories();
        // Create a mark file to stop this dmfile from being removed by GC.
        // We should create NGC file before creating the file under single file mode,
        // or the file may be removed.
        // FIXME : this should not use PageUtils.
        PageUtil::touchFile(new_dmfile->ngcPath());
        PageUtil::touchFile(path);
    }
    else
    {
        file.createDirectories();
        // Create a mark file to stop this dmfile from being removed by GC.
        // We should create NGC file after creating the directory under folder mode
        // since the NGC file is a file under the folder.
        // FIXME : this should not use PageUtils.
        PageUtil::touchFile(new_dmfile->ngcPath());
    }

    return new_dmfile;
}

DMFilePtr DMFile::restore(
    const FileProviderPtr & file_provider,
    UInt64 file_id,
    UInt64 page_id,
    const String & parent_path,
    const ReadMetaMode & read_meta_mode)
{
    String path = getPathByStatus(parent_path, file_id, DMFile::Status::READABLE);
    // The path may be dropped by another thread in some cases
    auto poco_file = Poco::File(path);
    if (!poco_file.exists())
        return nullptr;

    bool single_file_mode = poco_file.isFile();
    DMFilePtr dmfile(new DMFile(
        file_id,
        page_id,
        parent_path,
        single_file_mode ? Mode::SINGLE_FILE : Mode::FOLDER,
        Status::READABLE,
        &Poco::Logger::get("DMFile")));
    if (!read_meta_mode.isNone())
    {
        if (!single_file_mode)
        {
            dmfile->readConfiguration(file_provider);
        }
        dmfile->readMetadata(file_provider, read_meta_mode);
    }
    return dmfile;
}

String DMFile::colIndexCacheKey(const FileNameBase & file_name_base) const
{
    if (isSingleFileMode())
    {
        return path() + "/" + DMFile::colIndexFileName(file_name_base);
    }
    else
    {
        return colIndexPath(file_name_base);
    }
}

String DMFile::colMarkCacheKey(const FileNameBase & file_name_base) const
{
    if (isSingleFileMode())
    {
        return path() + "/" + DMFile::colMarkFileName(file_name_base);
    }
    else
    {
        return colMarkPath(file_name_base);
    }
}

bool DMFile::isColIndexExist(const ColId & col_id) const
{
    if (isSingleFileMode())
    {
        const auto index_identifier = DMFile::colIndexFileName(DMFile::getFileNameBase(col_id));
        return isSubFileExists(index_identifier);
    }
    else
    {
        return column_indices.count(col_id) != 0;
    }
}

String DMFile::encryptionBasePath() const
{
    return getPathByStatus(parent_path, file_id, DMFile::Status::READABLE);
}


EncryptionPath DMFile::encryptionDataPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : file_name_base + details::DATA_FILE_SUFFIX);
}

EncryptionPath DMFile::encryptionIndexPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : file_name_base + details::INDEX_FILE_SUFFIX);
}

EncryptionPath DMFile::encryptionMarkPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : file_name_base + details::MARK_FILE_SUFFIX);
}

EncryptionPath DMFile::encryptionMetaPath() const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : metaFileName());
}

EncryptionPath DMFile::encryptionPackStatPath() const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : packStatFileName());
}

EncryptionPath DMFile::encryptionPackPropertyPath() const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : packPropertyFileName());
}

EncryptionPath DMFile::encryptionConfigurationPath() const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : configurationFileName());
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
        WriteBufferFromFileProvider buf(file_provider, tmp_property_path, encryptionPackPropertyPath(), false, write_limiter, 4096);
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
    if (unlikely(mode != Mode::FOLDER))
    {
        throw DB::TiFlashException("upgradeMetaIfNeed is only expected to be called when mode is FOLDER.", Errors::DeltaTree::Internal);
    }
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
                throw TiFlashException(fmt::format("checksum mismatch for {}", metaPath()), Errors::Checksum::DataCorruption);
            }
            buf = &meta_reader;
        }
        else
        {
            log->warning(fmt::format("checksum for {} not found", name));
        }
    }

    DMFileFormat::Version ver; // Binary version
    assertString("DTFile format: ", *buf);
    DB::readText(ver, *buf);
    assertString("\n", *buf);
    readText(column_stats, ver, *buf);

    // No need to upgrade meta when mode is Mode::SINGLE_FILE
    if (mode == Mode::FOLDER)
    {
        // for V2, we do not apply in-place upgrade for now
        // but it should not affect the normal read procedure
        if (unlikely(ver >= DMFileFormat::V2 && !configuration))
        {
            throw TiFlashException("configuration expected but not loaded", Errors::Checksum::Missing);
        }
        upgradeMetaIfNeed(file_provider, ver);
    }
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
        if (sizeof(PackStat) * packs != buf->readBig(reinterpret_cast<char *>(pack_stats.data()), sizeof(PackStat) * packs))
        {
            throw Exception("Cannot read all data", ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
    else
    {
        auto buf = openForRead(file_provider, path, encryptionPackStatPath(), meta_pack_info.pack_stat_size);
        buf.seek(meta_pack_info.pack_stat_offset);
        if (sizeof(PackStat) * packs != buf.readBig(reinterpret_cast<char *>(pack_stats.data()), sizeof(PackStat) * packs))
        {
            throw Exception("Cannot read all data", ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
}

void DMFile::readConfiguration(const FileProviderPtr & file_provider)
{
    if (Poco::File(configurationPath()).exists())
    {
        auto file = openForRead(file_provider, configurationPath(), encryptionConfigurationPath(), DBMS_DEFAULT_BUFFER_SIZE);
        auto stream = InputStreamWrapper{file};
        configuration.emplace(stream);
    }
    else
    {
        configuration.reset();
    }
}


void DMFile::readPackProperty(const FileProviderPtr & file_provider, const MetaPackInfo & meta_pack_info)
{
    String tmp_buf;
    const auto name = packPropertyFileName();
    auto buf = openForRead(file_provider, packPropertyPath(), encryptionPackPropertyPath(), meta_pack_info.pack_property_size);
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
                throw TiFlashException(fmt::format("checksum mismatch for {}", packPropertyPath()), Errors::Checksum::DataCorruption);
            }
        }
        else
        {
            log->warning(fmt::format("checksum for {} not found", name));
        }
    }
}

void DMFile::readMetadata(const FileProviderPtr & file_provider, const ReadMetaMode & read_meta_mode)
{
    Footer footer;
    if (isSingleFileMode())
    {
        // Read the `Footer` part from disk and init `sub_file_stat`
        /// TODO: Redesign the file format for single file mode (https://github.com/pingcap/tics/issues/1798)
        Poco::File file(path());
        ReadBufferFromFileProvider buf(file_provider, path(), EncryptionPath(encryptionBasePath(), ""));

        buf.seek(file.getSize() - sizeof(Footer), SEEK_SET);
        DB::readIntBinary(footer.meta_pack_info.pack_property_offset, buf);
        DB::readIntBinary(footer.meta_pack_info.pack_property_size, buf);
        DB::readIntBinary(footer.meta_pack_info.column_stat_offset, buf);
        DB::readIntBinary(footer.meta_pack_info.column_stat_size, buf);
        DB::readIntBinary(footer.meta_pack_info.pack_stat_offset, buf);
        DB::readIntBinary(footer.meta_pack_info.pack_stat_size, buf);
        DB::readIntBinary(footer.sub_file_stat_offset, buf);
        DB::readIntBinary(footer.sub_file_num, buf);
        // initialize sub file state
        buf.seek(footer.sub_file_stat_offset, SEEK_SET);
        SubFileStat sub_file_stat{};
        for (UInt32 i = 0; i < footer.sub_file_num; i++)
        {
            String name;
            DB::readStringBinary(name, buf);
            DB::readIntBinary(sub_file_stat.offset, buf);
            DB::readIntBinary(sub_file_stat.size, buf);
            sub_file_stats.emplace(name, sub_file_stat);
        }
    }
    else
    {
        if (read_meta_mode.isAll())
        {
            initializeSubFileStatsForFolderMode();
            initializeIndices();
        }
        if (auto file = Poco::File(packPropertyPath()); file.exists())
            footer.meta_pack_info.pack_property_size = file.getSize();

        auto recheck = [&](size_t size) {
            if (this->configuration)
            {
                auto total_size = this->configuration->getChecksumFrameLength() + this->configuration->getChecksumHeaderLength();
                auto frame_count = size / total_size
                    + (0 != size % total_size);
                size -= frame_count * this->configuration->getChecksumHeaderLength();
            }
            return size;
        };

        if (auto file = Poco::File(packPropertyPath()); file.exists())
            footer.meta_pack_info.pack_property_size = file.getSize();

        footer.meta_pack_info.column_stat_size = Poco::File(metaPath()).getSize();
        footer.meta_pack_info.pack_stat_size = recheck(Poco::File(packStatPath()).getSize());
    }

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
        log->warning("checksum disabled due to lack of configuration");
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
    initializeSubFileStatsForFolderMode();
    initializeIndices();
}

void DMFile::finalizeForSingleFileMode(WriteBuffer & buffer)
{
    Footer footer;
    std::tie(footer.meta_pack_info.pack_property_offset, footer.meta_pack_info.pack_property_size) = writePackPropertyToBuffer(buffer);
    std::tie(footer.meta_pack_info.column_stat_offset, footer.meta_pack_info.column_stat_size) = writeMetaToBuffer(buffer);
    std::tie(footer.meta_pack_info.pack_stat_offset, footer.meta_pack_info.pack_stat_size) = writePackStatToBuffer(buffer);

    footer.sub_file_stat_offset = buffer.count();
    footer.sub_file_num = sub_file_stats.size();
    for (auto & iter : sub_file_stats)
    {
        writeStringBinary(iter.first, buffer);
        writeIntBinary(iter.second.offset, buffer);
        writeIntBinary(iter.second.size, buffer);
    }
    writeIntBinary(footer.meta_pack_info.pack_property_offset, buffer);
    writeIntBinary(footer.meta_pack_info.pack_property_size, buffer);
    writeIntBinary(footer.meta_pack_info.column_stat_offset, buffer);
    writeIntBinary(footer.meta_pack_info.column_stat_size, buffer);
    writeIntBinary(footer.meta_pack_info.pack_stat_offset, buffer);
    writeIntBinary(footer.meta_pack_info.pack_stat_size, buffer);
    writeIntBinary(footer.sub_file_stat_offset, buffer);
    writeIntBinary(footer.sub_file_num, buffer);
    writeIntBinary(static_cast<std::underlying_type_t<DMSingleFileFormatVersion>>(footer.file_format_version), buffer);
    buffer.next();
    if (status != Status::WRITING)
        throw Exception(fmt::format("Expected WRITING status, now {}", statusString(status)));
    Poco::File old_file(path());
    Poco::File old_ngc_file(ngcPath());

    setStatus(Status::READABLE);

    auto new_path = path();
    Poco::File file(new_path);
    if (file.exists())
        file.remove();
    Poco::File new_ngc_file(ngcPath());
    new_ngc_file.createFile();
    old_file.renameTo(new_path);
    old_ngc_file.remove();
}

std::set<UInt64> DMFile::listAllInPath(
    const FileProviderPtr & file_provider,
    const String & parent_path,
    const DMFile::ListOptions & options)
{
    Poco::File folder(parent_path);
    if (!folder.exists())
        return {};
    std::vector<std::string> file_names;
    folder.list(file_names);
    std::set<UInt64> file_ids;
    Poco::Logger * log = &Poco::Logger::get("DMFile");

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
                    LOG_INFO(log, "Unrecognized temporary or dropped dmfile, ignored: {}", name);
                    continue;
                }
                UInt64 file_id = *res;
                const String readable_path = getPathByStatus(parent_path, file_id, DMFile::Status::READABLE);
                file_provider->deleteEncryptionInfo(EncryptionPath(readable_path, ""), /* throw_on_error= */ false);
                const auto full_path = parent_path + "/" + name;
                if (Poco::File file(full_path); file.exists())
                    file.remove(true);
                LOG_WARNING(log, "Existing temporary or dropped dmfile, removed: {}", full_path);
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
            LOG_INFO(log, "Unrecognized DM file, ignored: {}", name);
            continue;
        }
        UInt64 file_id = *res;

        if (options.only_list_can_gc)
        {
            // Only return the ID if the file is able to be GC-ed.
            const auto file_path = parent_path + "/" + name;
            Poco::File file(file_path);
            String ngc_path = details::getNGCPath(file_path, file.isFile());
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

bool DMFile::canGC()
{
    return !Poco::File(ngcPath()).exists();
}

void DMFile::enableGC()
{
    Poco::File ngc_file(ngcPath());
    if (ngc_file.exists())
        ngc_file.remove();
}

void DMFile::remove(const FileProviderPtr & file_provider)
{
    if (isSingleFileMode())
    {
        file_provider->deleteRegularFile(path(), EncryptionPath(encryptionBasePath(), ""));
    }
    else
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
}

void DMFile::initializeSubFileStatsForFolderMode()
{
    if (isSingleFileMode())
        return;

    Poco::File directory{path()};
    std::vector<std::string> sub_files{};
    directory.list(sub_files);
    for (const auto & name : sub_files)
    {
        if (endsWith(name, details::DATA_FILE_SUFFIX) || endsWith(name, details::INDEX_FILE_SUFFIX)
            || endsWith(name, details::MARK_FILE_SUFFIX))
        {
            auto size = Poco::File(path() + "/" + name).getSize();
            sub_file_stats.emplace(name, SubFileStat{0, size});
        }
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
    if (isSingleFileMode())
        return;

    Poco::File directory{path()};
    std::vector<std::string> sub_files{};
    directory.list(sub_files);
    for (const auto & name : sub_files)
    {
        if (endsWith(name, details::INDEX_FILE_SUFFIX))
        {
            column_indices.insert(decode(removeSuffix(name, strlen(details::INDEX_FILE_SUFFIX)))); // strip tailing `.idx`
        }
    }
}

} // namespace DM
} // namespace DB
