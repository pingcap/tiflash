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

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <IO/Buffer/WriteBufferFromOStream.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <IO/FileProvider/ChecksumWriteBufferBuilder.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>


namespace DB::DM
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
        Stream(
            const DMFilePtr & dmfile,
            const String & file_base_name,
            const DataTypePtr & type,
            CompressionSettings compression_settings,
            size_t max_compress_block_size,
            FileProviderPtr & file_provider,
            const WriteLimiterPtr & write_limiter_,
            bool do_index)
            : plain_file(ChecksumWriteBufferBuilder::build(
                dmfile->getConfiguration().has_value(),
                file_provider,
                dmfile->colDataPath(file_base_name),
                dmfile->encryptionDataPath(file_base_name),
                false,
                write_limiter_,
                detail::getAlgorithmOrNone(*dmfile),
                detail::getFrameSizeOrDefault(*dmfile),
                /*flags*/ -1,
                /*mode*/ 0666,
                max_compress_block_size))
            , minmaxes(do_index ? std::make_shared<MinMaxIndex>(*type) : nullptr)
        {
            assert(compression_settings.settings.size() == 1);
            auto setting = getCompressionSetting(type, file_base_name, compression_settings.settings[0]);
            compressed_buf = CompressedWriteBuffer<>::build(
                *plain_file,
                CompressionSettings(setting),
                !dmfile->getConfiguration());

            if (!dmfile->useMetaV2())
            {
                // will not used in DMFileFormat::V3, could be removed when v3 is default
                mark_file = ChecksumWriteBufferBuilder::build(
                    dmfile->getConfiguration().has_value(),
                    file_provider,
                    dmfile->colMarkPath(file_base_name),
                    dmfile->encryptionMarkPath(file_base_name),
                    false,
                    write_limiter_,
                    detail::getAlgorithmOrNone(*dmfile),
                    detail::getFrameSizeOrDefault(*dmfile));
            }
            else
            {
                marks = std::make_shared<MarksInCompressedFile>();
            }
        }

        static bool isStringSizes(const DataTypePtr & type, const String & file_base_name)
        {
            return removeNullable(type)->getTypeId() == TypeIndex::String && file_base_name.ends_with(".size");
        }

        static CompressionSetting getCompressionSetting(
            const DataTypePtr & type,
            const String & file_base_name,
            const CompressionSetting & setting)
        {
            // Force use Lightweight compression for string sizes, since the string sizes almost always small.
            // Performance of LZ4 to decompress such integers is not good.
            return isStringSizes(type, file_base_name)
                ? CompressionSetting{CompressionMethod::Lightweight, CompressionDataType::Int64}
                : CompressionSetting::create<>(setting.method, setting.level, *type);
        }

        // compressed_buf -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        WriteBufferPtr compressed_buf;

        MinMaxIndexPtr minmaxes;

        MarksInCompressedFilePtr marks;

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

        Options(
            CompressionSettings compression_settings_,
            size_t min_compress_block_size_,
            size_t max_compress_block_size_)
            : compression_settings(compression_settings_)
            , min_compress_block_size(min_compress_block_size_)
            , max_compress_block_size(max_compress_block_size_)
        {}

        Options(const Options & from) = default;
    };


public:
    DMFileWriter(
        const DMFilePtr & dmfile_,
        const ColumnDefines & write_columns_,
        const FileProviderPtr & file_provider_,
        const WriteLimiterPtr & write_limiter_,
        const Options & options_);

    void write(const Block & block, const BlockProperty & block_property);
    void finalize();

    DMFilePtr getFile() const { return dmfile; }

private:
    void finalizeColumn(ColId col_id, DataTypePtr type);
    void writeColumn(
        ColId col_id,
        const IDataType & type,
        const IColumn & column,
        const ColumnVector<UInt8> * del_mark);

    /// Add streams with specified column id. Since a single column may have more than one Stream,
    /// for example Nullable column has a NullMap column, we would track them with a mapping
    /// FileNameBase -> Stream.
    void addStreams(ColId col_id, DataTypePtr type, bool do_index);

    WriteBufferFromFileBasePtr createMetaFile();
    void finalizeMeta();

private:
    DMFilePtr dmfile;
    ColumnDefines write_columns;
    Options options;

    ColumnStreams column_streams;

    FileProviderPtr file_provider;
    WriteLimiterPtr write_limiter;

    // If dmfile->useMetaV2() is true, `meta_file` is for metav2,
    // else `meta_file` is for pack stats.
    WriteBufferFromFileBasePtr meta_file;

    DMFileMetaV2::MergedFileWriter merged_file;

    // use to avoid count data written in index file for empty dmfile
    bool is_empty_file = true;
};

} // namespace DB::DM
