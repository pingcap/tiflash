#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>

namespace DB
{
namespace DM
{

class DMFileWriter
{
public:
    using WriteBufferFromFileBasePtr = std::unique_ptr<WriteBufferFromFileBase>;

    struct Stream
    {
        Stream(const DMFilePtr &   dmfile,
               const String &      file_base_name,
               const DataTypePtr & type,
               CompressionSettings compression_settings,
               size_t              max_compress_block_size,
               FileProviderPtr &   file_provider,
               bool                do_index)
            : plain_file(createWriteBufferFromFileBaseByFileProvider(file_provider,
                                                                     dmfile->colDataPath(file_base_name),
                                                                     dmfile->encryptionDataPath(file_base_name),
                                                                     false,
                                                                     0,
                                                                     0,
                                                                     max_compress_block_size)),
              plain_hashing(*plain_file),
              compressed_buf(plain_hashing, compression_settings),
              original_hashing(compressed_buf),
              minmaxes(do_index ? std::make_shared<MinMaxIndex>(*type) : nullptr),
              mark_file(file_provider, dmfile->colMarkPath(file_base_name), dmfile->encryptionMarkPath(file_base_name), false)
        {
        }

        void flush()
        {
            // Note that this method won't flush minmaxes.
            original_hashing.next();
            compressed_buf.next();
            plain_hashing.next();
            plain_file->next();

            plain_file->sync();
            mark_file.sync();
        }

        // Get written bytes of `plain_file` && `mark_file`. Should be called after `flush`.
        // Note that this class don't take responsible for serializing `minmaxes`,
        // bytes of `minmaxes` won't be counted in this method.
        size_t getWrittenBytes() { return plain_file->getPositionInFile() + mark_file.getPositionInFile(); }

        /// original_hashing -> compressed_buf -> plain_hashing -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        HashingWriteBuffer         plain_hashing;
        CompressedWriteBuffer      compressed_buf;
        HashingWriteBuffer         original_hashing;

        MinMaxIndexPtr              minmaxes;
        WriteBufferFromFileProvider mark_file;
    };
    using StreamPtr     = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

    struct SingleFileStream
    {
        SingleFileStream(const DMFilePtr &       dmfile,
                         CompressionSettings     compression_settings,
                         size_t                  max_compress_block_size,
                         const FileProviderPtr & file_provider)
            : plain_file(createWriteBufferFromFileBaseByFileProvider(
                file_provider, dmfile->path(), EncryptionPath(dmfile->encryptionBasePath(), ""), true, 0, 0, max_compress_block_size)),
              plain_hashing(*plain_file),
              compressed_buf(plain_hashing, compression_settings),
              original_hashing(compressed_buf)
        {
        }

        void flushCompressedData()
        {
            original_hashing.next();
            compressed_buf.next();
        }

        void flush()
        {
            plain_hashing.next();
            plain_file->next();

            plain_file->sync();
        }

        using ColumnMinMaxIndexs = std::unordered_map<String, MinMaxIndexPtr>;
        ColumnMinMaxIndexs minmax_indexs;

        using ColumnDataSizes = std::unordered_map<String, size_t>;
        ColumnDataSizes column_data_sizes;

        using MarkWithSizes       = std::vector<MarkWithSizeInCompressedFile>;
        using ColumnMarkWithSizes = std::unordered_map<String, MarkWithSizes>;
        ColumnMarkWithSizes column_mark_with_sizes;

        /// original_hashing -> compressed_buf -> plain_hashing -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        HashingWriteBuffer         plain_hashing;
        CompressedWriteBuffer      compressed_buf;
        HashingWriteBuffer         original_hashing;
    };
    using SingleFileStreamPtr = std::shared_ptr<SingleFileStream>;

public:
    DMFileWriter(const DMFilePtr &           dmfile_,
                 const ColumnDefines &       write_columns_,
                 size_t                      min_compress_block_size_,
                 size_t                      max_compress_block_size_,
                 const CompressionSettings & compression_settings_,
                 const FileProviderPtr &     file_provider_,
                 bool                        single_file_mode_ = false);

    void write(const Block & block, size_t not_clean_rows);
    void finalize();

private:
    void finalizeColumn(ColId col_id, DataTypePtr type);
    void writeColumn(ColId col_id, const IDataType & type, const IColumn & column, const ColumnVector<UInt8> * del_mark);

    /// Add streams with specified column id. Since a single column may have more than one Stream,
    /// for example Nullable column has a NullMap column, we would track them with a mapping
    /// FileNameBase -> Stream.
    void addStreams(ColId col_id, DataTypePtr type, bool do_index);

private:
    DMFilePtr           dmfile;
    ColumnDefines       write_columns;
    size_t              min_compress_block_size;
    size_t              max_compress_block_size;
    CompressionSettings compression_settings;

    ColumnStreams column_streams;

    WriteBufferFromFileBasePtr pack_stat_file;

    SingleFileStreamPtr single_file_stream;

    FileProviderPtr file_provider;
    bool            single_file_mode;
};

} // namespace DM
} // namespace DB
