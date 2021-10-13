#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferProxy.h>
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
            : plain_file(createWriteBufferFromFileBaseByFileProvider(
                WriteBufferParameterTable{
                    .has_checksum = dmfile->configuration.has_value(),
                    .file_provider = file_provider,
                    .filename = dmfile->colDataPath(file_base_name),
                    .encryption_path = dmfile->encryptionDataPath(file_base_name),
                    .create_new_encryption_info = false,
                    .write_limiter = write_limiter_,
                    .buffer_size = max_compress_block_size,
                    .checksum_algorithm = detail::getAlgorithmOrNone(*dmfile),
                    .checksum_frame_size = detail::getFrameSizeOrDefault(*dmfile)}))
            , plain_layer(*plain_file)
            , compressed_buf(dmfile->configuration
                                 ? std::unique_ptr<WriteBuffer>(new CompressedWriteBuffer<false>(plain_layer, compression_settings))
                                 : std::unique_ptr<WriteBuffer>(new CompressedWriteBuffer<true>(plain_layer, compression_settings)))
            , original_layer(*compressed_buf)
            , minmaxes(do_index ? std::make_shared<MinMaxIndex>(*type) : nullptr)
            , mark_file(createWriteBufferFromFileBaseByFileProvider(
                  WriteBufferParameterTable{
                      .has_checksum = dmfile->configuration.has_value(),
                      .file_provider = file_provider,
                      .filename = dmfile->colMarkPath(file_base_name),
                      .encryption_path = dmfile->encryptionMarkPath(file_base_name),
                      .create_new_encryption_info = false,
                      .write_limiter = write_limiter_,
                      .checksum_algorithm = detail::getAlgorithmOrNone(*dmfile),
                      .checksum_frame_size = detail::getFrameSizeOrDefault(*dmfile)}))
        {
        }

        void flush()
        {
            // Note that this method won't flush minmaxes.
            original_layer.next();
            compressed_buf->next();
            plain_layer.next();
            plain_file->next();

            plain_file->sync();
            mark_file->sync();
        }

        // Get written bytes of `plain_file` && `mark_file`. Should be called after `flush`.
        // Note that this class don't take responsible for serializing `minmaxes`,
        // bytes of `minmaxes` won't be counted in this method.
        size_t getWrittenBytes() { return plain_file->getPositionInFile() + mark_file->getPositionInFile(); }

        /// original_layer -> compressed_buf -> plain_layer -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        WriteBufferProxy plain_layer;
        WriteBufferPtr compressed_buf;
        WriteBufferProxy original_layer;

        MinMaxIndexPtr minmaxes;
        WriteBufferFromFileBasePtr mark_file;
    };
    using StreamPtr = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

    struct SingleFileStream
    {
        SingleFileStream(const DMFilePtr & dmfile,
                         CompressionSettings compression_settings,
                         size_t max_compress_block_size,
                         const FileProviderPtr & file_provider,
                         const WriteLimiterPtr & write_limiter_)
            : plain_file(createWriteBufferFromFileBaseByFileProvider(file_provider,
                                                                     dmfile->path(),
                                                                     EncryptionPath(dmfile->encryptionBasePath(), ""),
                                                                     true,
                                                                     write_limiter_,
                                                                     0,
                                                                     0,
                                                                     max_compress_block_size))
            , plain_layer(*plain_file)
            , compressed_buf(plain_layer, compression_settings)
            , original_layer(compressed_buf)
        {
        }

        void flushCompressedData()
        {
            original_layer.next();
            compressed_buf.next();
        }

        void flush()
        {
            plain_layer.next();
            plain_file->next();

            plain_file->sync();
        }

        using ColumnMinMaxIndexs = std::unordered_map<String, MinMaxIndexPtr>;
        ColumnMinMaxIndexs minmax_indexs;

        using ColumnDataSizes = std::unordered_map<String, size_t>;
        ColumnDataSizes column_data_sizes;

        using MarkWithSizes = std::vector<MarkWithSizeInCompressedFile>;
        using ColumnMarkWithSizes = std::unordered_map<String, MarkWithSizes>;
        ColumnMarkWithSizes column_mark_with_sizes;

        /// original_layer -> compressed_buf -> plain_layer -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        HashingWriteBuffer plain_layer;
        CompressedWriteBuffer<> compressed_buf;
        HashingWriteBuffer original_layer;
    };
    using SingleFileStreamPtr = std::shared_ptr<SingleFileStream>;

    struct BlockProperty
    {
        size_t not_clean_rows;
        size_t effective_num_rows;
        size_t gc_hint_version;
    };

    struct Flags
    {
    private:
        static constexpr size_t IS_SINGLE_FILE = 0x01;

        size_t value;

    public:
        Flags()
            : value(0x0)
        {}

        inline void setSingleFile(bool v) { value = (v ? (value | IS_SINGLE_FILE) : (value & ~IS_SINGLE_FILE)); }
        inline bool isSingleFile() const { return (value & IS_SINGLE_FILE); }
    };

    struct Options
    {
        CompressionSettings compression_settings;
        size_t min_compress_block_size;
        size_t max_compress_block_size;
        Flags flags;

        Options() = default;

        Options(CompressionSettings compression_settings_, size_t min_compress_block_size_, size_t max_compress_block_size_, Flags flags_)
            : compression_settings(compression_settings_)
            , min_compress_block_size(min_compress_block_size_)
            , max_compress_block_size(max_compress_block_size_)
            , flags(flags_)
        {
        }

        Options(const Options & from, const DMFilePtr & file)
            : compression_settings(from.compression_settings)
            , min_compress_block_size(from.min_compress_block_size)
            , max_compress_block_size(from.max_compress_block_size)
            , flags(from.flags)
        {
            flags.setSingleFile(file->isSingleFileMode());
        }
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

    SingleFileStreamPtr single_file_stream;

    FileProviderPtr file_provider;
    WriteLimiterPtr write_limiter;
};

} // namespace DM
} // namespace DB
