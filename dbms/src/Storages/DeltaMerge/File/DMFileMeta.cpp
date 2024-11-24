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

#include <Common/StringUtils/StringRefUtils.h>
#include <Common/escapeForFileName.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>
#include <IO/IOSWrapper.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/DMFileMeta.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace DB::ErrorCodes

namespace DB::DM
{

void DMFileMeta::initializeIndices()
{
    auto decode = [](const StringRef & data) {
        try
        {
            auto original = unescapeForFileName(data);
            return std::stoll(original);
        }
        catch (const std::invalid_argument & err)
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "invalid ColId: {} from file: {}", err.what(), data);
        }
        catch (const std::out_of_range & err)
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "invalid ColId: {} from file: {}", err.what(), data);
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

void DMFileMeta::read(const FileProviderPtr & file_provider, const DMFileMeta::ReadMode & read_meta_mode)
{
    readConfiguration(file_provider);
    if (read_meta_mode.isAll())
    {
        initializeIndices();
    }

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

    size_t pack_property_size = 0;
    if (auto file = Poco::File(packPropertyPath()); file.exists())
        pack_property_size = file.getSize();
    size_t column_stat_size = Poco::File(metaPath()).getSize();
    size_t pack_stat_size = recheck(Poco::File(packStatPath()).getSize());

    if (read_meta_mode.needPackProperty() && pack_property_size != 0)
        readPackProperty(file_provider, pack_property_size);

    if (read_meta_mode.needColumnStat())
        readColumnStat(file_provider, column_stat_size);

    if (read_meta_mode.needPackStat())
        readPackStat(file_provider, pack_stat_size);
}

void DMFileMeta::readColumnStat(const FileProviderPtr & file_provider, size_t size)
{
    const auto name = metaFileName();
    auto file_buf = openForRead(
        file_provider,
        metaPath(),
        encryptionMetaPath(),
        std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), size));
    auto meta_buf = std::vector<char>(size);
    auto meta_reader = ReadBufferFromMemory{meta_buf.data(), meta_buf.size()};
    ReadBuffer * buf = &file_buf;

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
                throw TiFlashException(Errors::Checksum::DataCorruption, "checksum mismatch for {}", metaPath());
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
    tryUpgradeColumnStatInMetaV1(file_provider, ver);
}

void DMFileMeta::readPackStat(const FileProviderPtr & file_provider, size_t size)
{
    RUNTIME_CHECK_MSG(size % sizeof(DMFileMeta::PackStat) == 0, "Invalid pack stat size: {}", size);
    const size_t packs = size / sizeof(DMFileMeta::PackStat);
    pack_stats.resize(packs);
    const auto path = packStatPath();
    if (configuration)
    {
        auto buf = ChecksumReadBufferBuilder::build(
            file_provider,
            path,
            encryptionPackStatPath(),
            configuration->getChecksumFrameLength(),
            nullptr,
            configuration->getChecksumAlgorithm(),
            configuration->getChecksumFrameLength());
        if (size != buf->readBig(reinterpret_cast<char *>(pack_stats.data()), size))
        {
            throw Exception("Cannot read all data", ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
    else
    {
        auto buf = openForRead(file_provider, path, encryptionPackStatPath(), size);
        if (size != buf.readBig(reinterpret_cast<char *>(pack_stats.data()), size))
        {
            throw Exception("Cannot read all data", ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
}

void DMFileMeta::readConfiguration(const FileProviderPtr & file_provider)
{
    if (Poco::File(configurationPath()).exists())
    {
        auto buf
            = openForRead(file_provider, configurationPath(), encryptionConfigurationPath(), DBMS_DEFAULT_BUFFER_SIZE);
        auto stream = InputStreamWrapper{buf};
        configuration.emplace(stream);
        format_version = DMFileFormat::V2;
    }
    else
    {
        configuration.reset();
        format_version = DMFileFormat::V1;
    }
}

void DMFileMeta::readPackProperty(const FileProviderPtr & file_provider, size_t size)
{
    String tmp_buf;
    const auto name = packPropertyFileName();
    auto buf = openForRead(file_provider, packPropertyPath(), encryptionPackPropertyPath(), size);

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
                    Errors::Checksum::DataCorruption,
                    "checksum mismatch for {}",
                    packPropertyPath());
            }
        }
        else
        {
            LOG_WARNING(log, "checksum for {} not found", name);
        }
    }
}

void DMFileMeta::tryUpgradeColumnStatInMetaV1(const FileProviderPtr & file_provider, DMFileFormat::Version ver)
{
    if (likely(ver != DMFileFormat::V0))
        return;

    // Update ColumnStat.serialized_bytes
    for (auto && c : column_stats)
    {
        auto col_id = c.first;
        auto & stat = c.second;
        c.second.type->enumerateStreams(
            [col_id, &stat, this](const IDataType::SubstreamPath & substream) {
                String stream_name = DMFileMeta::getFileNameBase(col_id, substream);
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
    // Update ColumnStat in metaV1.
    writeMeta(file_provider, nullptr);
}

void DMFileMeta::writeMeta(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    String meta_path = metaPath();
    String tmp_meta_path = meta_path + ".tmp";

    {
        auto buf = WriteBufferFromWritableFileBuilder::build(
            file_provider,
            tmp_meta_path,
            encryptionMetaPath(),
            false,
            write_limiter,
            4096);
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

void DMFileMeta::writePackProperty(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    String property_path = packPropertyPath();
    String tmp_property_path = property_path + ".tmp";
    {
        auto buf = WriteBufferFromWritableFileBuilder::build(
            file_provider,
            tmp_property_path,
            encryptionPackPropertyPath(),
            false,
            write_limiter,
            4096);
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

void DMFileMeta::writeConfiguration(const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
{
    assert(configuration);
    String config_path = configurationPath();
    String tmp_config_path = config_path + ".tmp";
    {
        auto buf = WriteBufferFromWritableFileBuilder::build(
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

DMFileMeta::OffsetAndSize DMFileMeta::writeMetaToBuffer(WriteBuffer & buffer) const
{
    size_t meta_offset = buffer.count();
    writeString("DTFile format: ", buffer);
    writeIntText(configuration ? DMFileFormat::V2 : DMFileFormat::V1, buffer);
    writeString("\n", buffer);
    writeText(column_stats, STORAGE_FORMAT_CURRENT.dm_file, buffer);
    size_t meta_size = buffer.count() - meta_offset;
    return std::make_tuple(meta_offset, meta_size);
}

DMFileMeta::OffsetAndSize DMFileMeta::writePackPropertyToBuffer(WriteBuffer & buffer, UnifiedDigestBase * digest)
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

void DMFileMeta::finalize(
    WriteBuffer & buffer,
    const FileProviderPtr & file_provider,
    const WriteLimiterPtr & write_limiter)
{
    // write pack stats
    for (const auto & pack_stat : pack_stats)
    {
        writePODBinary(pack_stat, buffer);
    }
    writePackProperty(file_provider, write_limiter);
    writeMeta(file_provider, write_limiter);
    if (configuration)
    {
        writeConfiguration(file_provider, write_limiter);
    }
}

String DMFileMeta::encryptionBasePath() const
{
    return getPathByStatus(parent_path, file_id, DMFileStatus::READABLE);
}

EncryptionPath DMFileMeta::encryptionMetaPath() const
{
    return EncryptionPath(encryptionBasePath(), metaFileName(), keyspace_id);
}

EncryptionPath DMFileMeta::encryptionPackStatPath() const
{
    return EncryptionPath(encryptionBasePath(), packStatFileName(), keyspace_id);
}

EncryptionPath DMFileMeta::encryptionPackPropertyPath() const
{
    return EncryptionPath(encryptionBasePath(), packPropertyFileName(), keyspace_id);
}

EncryptionPath DMFileMeta::encryptionConfigurationPath() const
{
    return EncryptionPath(encryptionBasePath(), configurationFileName(), keyspace_id);
}

UInt64 DMFileMeta::getFileSize(ColId col_id, const String & filename) const
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

UInt64 DMFileMeta::getReadFileSize(ColId col_id, const String & filename) const
{
    return getFileSize(col_id, filename);
}

} // namespace DB::DM
