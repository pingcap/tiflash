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

#include <Storages/DeltaMerge/File/ColumnStream.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/Page/PageUtil.h>

namespace DB::DM
{
// TODO: make `MarkLoader` as a part of DMFile?
class MarkLoader
{
public:
    // Make the instance of `MarkLoader` as a callable object that is used in
    // `mark_cache->getOrSet(...)`.
    MarksInCompressedFilePtr operator()()
    {
        auto res = std::make_shared<MarksInCompressedFile>(reader.dmfile->getPacks());
        if (res->empty()) // 0 rows.
            return res;

        size_t size = sizeof(MarkInCompressedFile) * reader.dmfile->getPacks();
        auto mark_guard = S3::S3RandomAccessFile::setReadFileInfo({
            .size = reader.dmfile->getReadFileSize(col_id, colMarkFileName(file_name_base)),
            .scan_context = reader.scan_context,
        });

        if (likely(reader.dmfile->useMetaV2()))
        {
            // the col_mark is merged into metav2
            return loadColMarkFromMetav2To(res, size);
        }
        else if (unlikely(!reader.dmfile->getConfiguration()))
        {
            // without checksum, simply load the raw bytes from file
            return loadRawColMarkTo(res, size);
        }
        else
        {
            // checksum is enabled but not merged into meta v2
            return loadColMarkWithChecksumTo(res, size);
        }
    }

public:
    MarkLoader(
        DMFileReader & reader_,
        ColId col_id_,
        const String & file_name_base_,
        const ReadLimiterPtr & read_limiter_)
        : reader(reader_)
        , col_id(col_id_)
        , file_name_base(file_name_base_)
        , read_limiter(read_limiter_)
    {}

    DMFileReader & reader;
    ColId col_id;
    const String & file_name_base;
    ReadLimiterPtr read_limiter;

private:
    MarksInCompressedFilePtr loadRawColMarkTo(const MarksInCompressedFilePtr & res, size_t bytes_size)
    {
        auto file = reader.file_provider->newRandomAccessFile(
            reader.dmfile->colMarkPath(file_name_base),
            reader.dmfile->encryptionMarkPath(file_name_base));
        PageUtil::readFile(file, 0, reinterpret_cast<char *>(res->data()), bytes_size, read_limiter);
        return res;
    }
    MarksInCompressedFilePtr loadColMarkWithChecksumTo(const MarksInCompressedFilePtr & res, size_t bytes_size)
    {
        auto buffer = ChecksumReadBufferBuilder::build(
            reader.file_provider,
            reader.dmfile->colMarkPath(file_name_base),
            reader.dmfile->encryptionMarkPath(file_name_base),
            reader.dmfile->getConfiguration()->getChecksumFrameLength(),
            read_limiter,
            reader.dmfile->getConfiguration()->getChecksumAlgorithm(),
            reader.dmfile->getConfiguration()->getChecksumFrameLength());
        buffer->readBig(reinterpret_cast<char *>(res->data()), bytes_size);
        return res;
    }
    MarksInCompressedFilePtr loadColMarkFromMetav2To(const MarksInCompressedFilePtr & res, size_t bytes_size)
    {
        const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(reader.dmfile->meta.get());
        auto info = dmfile_meta->merged_sub_file_infos.find(colMarkFileName(file_name_base));
        if (info == dmfile_meta->merged_sub_file_infos.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown mark file {}", colMarkFileName(file_name_base));
        }

        auto file_path = dmfile_meta->mergedPath(info->second.number);
        auto encryp_path = dmfile_meta->encryptionMergedPath(info->second.number);
        auto offset = info->second.offset;
        auto data_size = info->second.size;

        if (data_size == 0)
            return res;

        // First, read from merged file to get the raw data(contains the header)
        auto buffer = ReadBufferFromRandomAccessFileBuilder::build(
            reader.file_provider,
            file_path,
            encryp_path,
            reader.dmfile->getConfiguration()->getChecksumFrameLength(),
            read_limiter);
        buffer.seek(offset);

        // Read the raw data into memory. It is OK because the mark merged into
        // merged_file is small enough.
        String raw_data;
        raw_data.resize(data_size);
        buffer.read(reinterpret_cast<char *>(raw_data.data()), data_size);

        // Then read from the buffer based on the raw data
        auto buf = ChecksumReadBufferBuilder::build(
            std::move(raw_data),
            reader.dmfile->colDataPath(file_name_base),
            reader.dmfile->getConfiguration()->getChecksumFrameLength(),
            reader.dmfile->getConfiguration()->getChecksumAlgorithm(),
            reader.dmfile->getConfiguration()->getChecksumFrameLength());
        buf->readBig(reinterpret_cast<char *>(res->data()), bytes_size);
        return res;
    }
};

std::unique_ptr<CompressedSeekableReaderBuffer> ColumnReadStream::buildColDataReadBuffWithoutChecksum(
    DMFileReader & reader,
    ColId col_id,
    const String & file_name_base,
    size_t n_packs,
    size_t max_read_buffer_size,
    const ReadLimiterPtr & read_limiter,
    const LoggerPtr & log) const
{
    assert(!reader.dmfile->getConfiguration());

    DMFile::ColDataType type = DMFile::ColDataType::Elements;
    if (endsWith(file_name_base, ".null"))
        type = DMFile::ColDataType::NullMap;
    else if (endsWith(file_name_base, ".size0"))
        type = DMFile::ColDataType::ArraySizes;
    size_t data_file_size = reader.dmfile->colDataSize(col_id, type);

    // Try to get the largest buffer size of reading continuous packs
    size_t buffer_size = 0;
    const auto & use_packs = reader.pack_filter.getUsePacksConst();
    for (size_t i = 0; i < n_packs; /*empty*/)
    {
        if (!use_packs[i])
        {
            ++i;
            continue;
        }
        size_t cur_offset_in_file = getOffsetInFile(i);
        size_t end = i + 1;
        // First, find the end of current available range.
        while (end < n_packs && use_packs[end])
            ++end;

        // Second, if the end of range is inside the block, we will need to read it too.
        if (end < n_packs)
        {
            size_t last_offset_in_file = getOffsetInFile(end);
            if (getOffsetInDecompressedBlock(end) > 0)
            {
                while (end < n_packs && getOffsetInFile(end) == last_offset_in_file)
                    ++end;
            }
        }

        size_t range_end_in_file = (end == n_packs) ? data_file_size : getOffsetInFile(end);

        size_t range = range_end_in_file - cur_offset_in_file;
        buffer_size = std::max(buffer_size, range);

        i = end;
    }
    buffer_size = std::min(buffer_size, max_read_buffer_size);

    LOG_TRACE(
        log,
        "col_id={} file_name_base={} file_size={} buffer_size={}",
        col_id,
        file_name_base,
        data_file_size,
        buffer_size);
    return CompressedReadBufferFromFileBuilder::buildLegacy(
        reader.file_provider,
        reader.dmfile->colDataPath(file_name_base),
        reader.dmfile->encryptionDataPath(file_name_base),
        read_limiter,
        buffer_size);
}

std::unique_ptr<CompressedSeekableReaderBuffer> ColumnReadStream::buildColDataReadBuffWitChecksum(
    DMFileReader & reader,
    [[maybe_unused]] ColId col_id,
    const String & file_name_base,
    const ReadLimiterPtr & read_limiter)
{
    return CompressedReadBufferFromFileBuilder::build(
        reader.file_provider,
        reader.dmfile->colDataPath(file_name_base),
        reader.dmfile->encryptionDataPath(file_name_base),
        reader.dmfile->getConfiguration()->getChecksumFrameLength(),
        read_limiter,
        reader.dmfile->getConfiguration()->getChecksumAlgorithm(),
        reader.dmfile->getConfiguration()->getChecksumFrameLength());
}

std::unique_ptr<CompressedSeekableReaderBuffer> ColumnReadStream::buildColDataReadBuffByMetaV2(
    DMFileReader & reader,
    [[maybe_unused]] ColId col_id,
    const String & file_name_base,
    const ReadLimiterPtr & read_limiter)
{
    const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(reader.dmfile->meta.get());
    auto info = dmfile_meta->merged_sub_file_infos.find(colDataFileName(file_name_base));
    if (info == dmfile_meta->merged_sub_file_infos.end())
    {
        return CompressedReadBufferFromFileBuilder::build(
            reader.file_provider,
            reader.dmfile->colDataPath(file_name_base),
            reader.dmfile->encryptionDataPath(file_name_base),
            reader.dmfile->getConfiguration()->getChecksumFrameLength(),
            read_limiter,
            reader.dmfile->getConfiguration()->getChecksumAlgorithm(),
            reader.dmfile->getConfiguration()->getChecksumFrameLength());
    }

    assert(info != dmfile_meta->merged_sub_file_infos.end());
    auto file_path = dmfile_meta->mergedPath(info->second.number);
    auto encryp_path = dmfile_meta->encryptionMergedPath(info->second.number);
    auto offset = info->second.offset;
    auto size = info->second.size;

    // First, read from merged file to get the raw data(contains the header)
    auto buffer = ReadBufferFromRandomAccessFileBuilder::build(
        reader.file_provider,
        file_path,
        encryp_path,
        reader.dmfile->getConfiguration()->getChecksumFrameLength(),
        read_limiter);
    buffer.seek(offset);

    // Read the raw data into memory. It is OK because the mark merged into
    // merged_file is small enough.
    String raw_data;
    raw_data.resize(size);
    buffer.read(reinterpret_cast<char *>(raw_data.data()), size);

    // Then read from the buffer based on the raw data
    return CompressedReadBufferFromFileBuilder::build(
        std::move(raw_data),
        file_path,
        reader.dmfile->getConfiguration()->getChecksumFrameLength(),
        reader.dmfile->getConfiguration()->getChecksumAlgorithm(),
        reader.dmfile->getConfiguration()->getChecksumFrameLength());
}

ColumnReadStream::ColumnReadStream(
    DMFileReader & reader,
    ColId col_id,
    const String & file_name_base,
    size_t max_read_buffer_size,
    const LoggerPtr & log,
    const ReadLimiterPtr & read_limiter)
    : avg_size_hint(reader.dmfile->getColumnStat(col_id).avg_size)
{
    // load mark data
    // TODO: Do we still need `marks` if the reader don't contains any pack?
    if (reader.mark_cache)
        marks = reader.mark_cache->getOrSet(
            reader.dmfile->colMarkCacheKey(file_name_base),
            MarkLoader(reader, col_id, file_name_base, read_limiter));
    else
        marks = MarkLoader(reader, col_id, file_name_base, read_limiter)();

    // skip empty dmfile
    size_t packs = reader.dmfile->getPacks();
    if (packs == 0)
        return;

    auto data_guard = S3::S3RandomAccessFile::setReadFileInfo({
        .size = reader.dmfile->getReadFileSize(col_id, colDataFileName(file_name_base)),
        .scan_context = reader.scan_context,
    });

    // load column data read buffer
    if (likely(reader.dmfile->useMetaV2()))
    {
        buf = buildColDataReadBuffByMetaV2(reader, col_id, file_name_base, read_limiter);
    }
    else if (unlikely(!reader.dmfile->getConfiguration())) // checksum not enabled
    {
        buf = buildColDataReadBuffWithoutChecksum(
            reader,
            col_id,
            file_name_base,
            packs,
            max_read_buffer_size,
            read_limiter,
            log);
    }
    else
    {
        buf = buildColDataReadBuffWitChecksum(reader, col_id, file_name_base, read_limiter);
    }
}

} // namespace DB::DM
