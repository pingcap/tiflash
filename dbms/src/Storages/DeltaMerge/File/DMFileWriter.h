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

#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>

namespace DB
{
namespace DM
{
namespace detail
{
static inline DB::ChecksumAlgo getAlgorithmOrNone(DMFile & dmfile)
{
    return dmfile.getConfiguration() ? dmfile.getConfiguration()->getChecksumAlgorithm() : ChecksumAlgo::None;
}
static inline size_t getFrameSizeOrDefault(DMFile & dmfile)
{
    return dmfile.getConfiguration() ? dmfile.getConfiguration()->getChecksumFrameLength() : DBMS_DEFAULT_BUFFER_SIZE;
}
} // namespace detail
class DMFileWriter
{
public:
    using WriteBufferFromFileBasePtr = std::unique_ptr<WriteBufferFromFileBase>;

    struct Stream
    {
        Stream(const DMFilePtr & dmfile,
               const String & file_base_name,
               const DataTypePtr & type,
               CompressionSettings compression_settings,
               size_t max_compress_block_size,
               FileProviderPtr & file_provider,
               const WriteLimiterPtr & write_limiter_,
               bool do_index)
            : plain_file(
                WriteBufferByFileProviderBuilder(
                    dmfile->configuration.has_value(),
                    file_provider,
                    dmfile->colDataPath(file_base_name),
                    dmfile->encryptionDataPath(file_base_name),
                    false,
                    write_limiter_)
                    .with_buffer_size(max_compress_block_size)
                    .with_checksum_algorithm(detail::getAlgorithmOrNone(*dmfile))
                    .with_checksum_frame_size(detail::getFrameSizeOrDefault(*dmfile))
                    .build())
            , compressed_buf(dmfile->configuration
                                 ? std::unique_ptr<WriteBuffer>(new CompressedWriteBuffer<false>(*plain_file, compression_settings))
                                 : std::unique_ptr<WriteBuffer>(new CompressedWriteBuffer<true>(*plain_file, compression_settings)))
            , minmaxes(do_index ? std::make_shared<MinMaxIndex>(*type) : nullptr)
            , mark_file(WriteBufferByFileProviderBuilder(
                            dmfile->configuration.has_value(),
                            file_provider,
                            dmfile->colMarkPath(file_base_name),
                            dmfile->encryptionMarkPath(file_base_name),
                            false,
                            write_limiter_)
                            .with_checksum_algorithm(detail::getAlgorithmOrNone(*dmfile))
                            .with_checksum_frame_size(detail::getFrameSizeOrDefault(*dmfile))
                            .build())
        {
        }

        void flush()
        {
            // Note that this method won't flush minmaxes.
            compressed_buf->next();
            plain_file->next();

            plain_file->sync();
            mark_file->sync();
        }

        // Get written bytes of `plain_file` && `mark_file`. Should be called after `flush`.
        // Note that this class don't take responsible for serializing `minmaxes`,
        // bytes of `minmaxes` won't be counted in this method.
        size_t getWrittenBytes() const { return plain_file->getMaterializedBytes() + mark_file->getMaterializedBytes(); }

        // compressed_buf -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        WriteBufferPtr compressed_buf;

        MinMaxIndexPtr minmaxes;
        WriteBufferFromFileBasePtr mark_file;
    };
    using StreamPtr = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

    struct BlockProperty
    {
        size_t not_clean_rows;
        size_t deleted_rows;
        size_t effective_num_rows;
        size_t gc_hint_version;
    };

    struct Options
    {
        CompressionSettings compression_settings;
        size_t min_compress_block_size{};
        size_t max_compress_block_size{};

        Options() = default;

        Options(CompressionSettings compression_settings_, size_t min_compress_block_size_, size_t max_compress_block_size_)
            : compression_settings(compression_settings_)
            , min_compress_block_size(min_compress_block_size_)
            , max_compress_block_size(max_compress_block_size_)
        {
        }

        Options(const Options & from) = default;
    };


public:
    DMFileWriter(const DMFilePtr & dmfile_,
                 const ColumnDefines & write_columns_,
                 const FileProviderPtr & file_provider_,
                 const WriteLimiterPtr & write_limiter_,
                 const Options & options_);

    void write(const Block & block, const BlockProperty & block_property);
    void finalize();

    const DMFilePtr getFile() const
    {
        return dmfile;
    }

private:
    void finalizeColumn(ColId col_id, DataTypePtr type);
    void writeColumn(ColId col_id, const IDataType & type, const IColumn & column, const ColumnVector<UInt8> * del_mark);

    /// Add streams with specified column id. Since a single column may have more than one Stream,
    /// for example Nullable column has a NullMap column, we would track them with a mapping
    /// FileNameBase -> Stream.
    void addStreams(ColId col_id, DataTypePtr type, bool do_index);

private:
    DMFilePtr dmfile;
    ColumnDefines write_columns;
    Options options;

    ColumnStreams column_streams;

    WriteBufferFromFileBasePtr pack_stat_file;

    FileProviderPtr file_provider;
    WriteLimiterPtr write_limiter;

    // use to avoid count data written in index file for empty dmfile
    bool is_empty_file = true;
};

} // namespace DM
} // namespace DB
