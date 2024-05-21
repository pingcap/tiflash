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

#include "DMFileMetaV2.h"

#include <IO/ReadBufferFromString.h>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
} // namespace DB::ErrorCodes

namespace DB::DM
{

EncryptionPath DMFileMetaV2::encryptionMetaPath() const
{
    return EncryptionPath(encryptionBasePath(), metaFileName());
}

EncryptionPath DMFileMetaV2::encryptionMergedPath(UInt32 number) const
{
    return EncryptionPath(encryptionBasePath(), mergedFilename(number));
}

void DMFileMetaV2::parse(std::string_view buffer)
{
    // Footer
    const auto * footer = reinterpret_cast<const Footer *>(buffer.data() + buffer.size() - sizeof(Footer));
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
        digest->update(buffer.data(), buffer.size() - sizeof(Footer) - hash_size);
        if (unlikely(!digest->compareRaw(ptr)))
        {
            LOG_ERROR(log, "{} checksum invalid", metaPath());
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{} checksum invalid", metaPath());
        }
    }

    ptr = ptr - sizeof(DMFileFormat::Version);
    version = *(reinterpret_cast<const DMFileFormat::Version *>(ptr));

    ptr = ptr - sizeof(UInt64);
    auto meta_block_handle_count = *(reinterpret_cast<const UInt64 *>(ptr));

    for (UInt64 i = 0; i < meta_block_handle_count; ++i)
    {
        ptr = ptr - sizeof(BlockHandle);
        const auto * handle = reinterpret_cast<const BlockHandle *>(ptr);
        // omit the default branch. If there are unknown MetaBlock (after in-place downgrade), just ignore and throw away
        switch (handle->type)
        {
        case BlockType::ColumnStat: // parse the `ColumnStat` from old version
            parseColumnStat(buffer.substr(handle->offset, handle->size));
            break;
        case BlockType::ExtendColumnStat:
            parseExtendColumnStat(buffer.substr(handle->offset, handle->size));
            break;
        case BlockType::PackProperty:
            parsePackProperty(buffer.substr(handle->offset, handle->size));
            break;
        case BlockType::PackStat:
            parsePackStat(buffer.substr(handle->offset, handle->size));
            break;
        case BlockType::MergedSubFilePos:
            parseMergedSubFilePos(buffer.substr(handle->offset, handle->size));
            break;
        }
    }
}

void DMFileMetaV2::parseColumnStat(std::string_view buffer)
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

void DMFileMetaV2::parseExtendColumnStat(std::string_view buffer)
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

void DMFileMetaV2::parsePackProperty(std::string_view buffer)
{
    const auto * pp = reinterpret_cast<const PackProperty *>(buffer.data());
    auto count = buffer.size() / sizeof(PackProperty);
    pack_properties.mutable_property()->Reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        pp[i].toProtobuf(pack_properties.add_property());
    }
}

void DMFileMetaV2::parsePackStat(std::string_view buffer)
{
    auto count = buffer.size() / sizeof(PackStat);
    pack_stats.resize(count);
    memcpy(reinterpret_cast<char *>(pack_stats.data()), buffer.data(), buffer.size());
}

void DMFileMetaV2::parseMergedSubFilePos(std::string_view buffer)
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

void DMFileMetaV2::finalize(
    WriteBuffer & buffer,
    const FileProviderPtr & /*file_provider*/,
    const WriteLimiterPtr & /*write_limiter*/)
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

    Footer footer{};
    if (configuration)
    {
        footer.checksum_algorithm = static_cast<UInt64>(configuration->getChecksumAlgorithm());
        footer.checksum_frame_length = configuration->getChecksumFrameLength();
    }
    writePODBinary(footer, buffer);
}

DMFileMeta::BlockHandle DMFileMetaV2::writeSLPackStatToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();
    const char * data = reinterpret_cast<const char *>(&pack_stats[0]);
    size_t size = pack_stats.size() * sizeof(PackStat);
    writeString(data, size, buffer);
    return BlockHandle{BlockType::PackStat, offset, buffer.count() - offset};
}

DMFileMeta::BlockHandle DMFileMetaV2::writeSLPackPropertyToBuffer(WriteBuffer & buffer) const
{
    auto offset = buffer.count();
    for (const auto & pb : pack_properties.property())
    {
        PackProperty tmp{pb};
        const char * data = reinterpret_cast<const char *>(&tmp);
        size_t size = sizeof(PackProperty);
        writeString(data, size, buffer);
    }
    return BlockHandle{BlockType::PackProperty, offset, buffer.count() - offset};
}

DMFileMeta::BlockHandle DMFileMetaV2::writeColumnStatToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();
    writeIntBinary(column_stats.size(), buffer);
    for (const auto & [id, stat] : column_stats)
    {
        stat.serializeToBuffer(buffer);
    }
    return BlockHandle{BlockType::ColumnStat, offset, buffer.count() - offset};
}

DMFileMeta::BlockHandle DMFileMetaV2::writeExtendColumnStatToBuffer(WriteBuffer & buffer)
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
    return BlockHandle{BlockType::ExtendColumnStat, offset, buffer.count() - offset};
}

DMFileMeta::BlockHandle DMFileMetaV2::writeMergedSubFilePosotionsToBuffer(WriteBuffer & buffer)
{
    auto offset = buffer.count();

    writeIntBinary(merged_files.size(), buffer);
    const auto * data = reinterpret_cast<const char *>(&merged_files[0]);
    auto bytes = merged_files.size() * sizeof(DMFileMetaV2::MergedFile);
    writeString(data, bytes, buffer);

    writeIntBinary(merged_sub_file_infos.size(), buffer);
    for (const auto & [fname, info] : merged_sub_file_infos)
    {
        info.serializeToBuffer(buffer);
    }
    return BlockHandle{BlockType::MergedSubFilePos, offset, buffer.count() - offset};
}

void DMFileMetaV2::read(const FileProviderPtr & file_provider, const ReadMode & /*read_meta_mode*/)
{
    auto rbuf = openForRead(file_provider, metaPath(), encryptionMetaPath(), meta_buffer_size);
    std::vector<char> buf(meta_buffer_size);
    size_t read_bytes = 0;
    for (;;)
    {
        read_bytes += rbuf.readBig(buf.data() + read_bytes, meta_buffer_size);
        if (likely(read_bytes < buf.size()))
        {
            break;
        }
        LOG_WARNING(log, "{}'s size is larger than {}", metaPath(), buf.size());
        buf.resize(buf.size() + meta_buffer_size);
    }
    buf.resize(read_bytes);
    std::string_view buffer(buf.data(), buf.size());
    parse(buffer);
}

void DMFileMetaV2::checkMergedFile(
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

        auto file = file_provider->newWritableFile(
            mergedPath(writer.file_info.number),
            encryptionMergedPath(writer.file_info.number),
            /*truncate_if_exists*/ true,
            /*create_new_encryption_info*/ false,
            write_limiter);
        writer.buffer = std::make_unique<WriteBufferFromWritableFile>(file);
    }
}

// Merge the small files into a single file to avoid
// filesystem inodes exhausting
void DMFileMetaV2::finalizeSmallFiles(
    MergedFileWriter & writer,
    FileProviderPtr & file_provider,
    WriteLimiterPtr & write_limiter)
{
    auto copy_file_to_cur = [&](const String & fname, UInt64 fsize) {
        checkMergedFile(writer, file_provider, write_limiter);

        auto file = openForRead(file_provider, subFilePath(fname), EncryptionPath(encryptionBasePath(), fname), fsize);
        std::vector<char> read_buf(fsize);
        auto read_size = file.readBig(read_buf.data(), read_buf.size());
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

UInt64 DMFileMetaV2::getReadFileSize(ColId col_id, const String & filename) const
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

UInt64 DMFileMetaV2::getMergedFileSizeOfColumn(const MergedSubFileInfo & file_info) const
{
    // Get filesize of merged file.
    auto itr = std::find_if(merged_files.begin(), merged_files.end(), [&file_info](const auto & merged_file) {
        return merged_file.number == file_info.number;
    });
    RUNTIME_CHECK(itr != merged_files.end());
    return itr->size;
}

} // namespace DB::DM
