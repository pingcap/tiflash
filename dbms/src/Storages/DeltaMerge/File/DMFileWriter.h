#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/createWriteBufferFromFileBase.h>
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
        Stream(const String &      path,
               const DataTypePtr & type,
               CompressionSettings compression_settings,
               size_t              max_compress_block_size,
               bool                do_index)
            : plain_file(createWriteBufferFromFileBase(path, 0, 0, max_compress_block_size)),
              plain_hashing(*plain_file),
              compressed_buf(plain_hashing, compression_settings),
              original_hashing(compressed_buf),
              minmaxes(do_index ? std::make_shared<MinMaxIndex>(*type) : nullptr),
              marks(std::make_shared<MarksInCompressedFile>())
        {
        }

        void finalize()
        {
            original_hashing.next();
            compressed_buf.next();
            plain_hashing.next();
            plain_file->next();
        }

        void sync() { plain_file->sync(); }

        /// original_hashing -> compressed_buf -> plain_hashing -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        HashingWriteBuffer         plain_hashing;
        CompressedWriteBuffer      compressed_buf;
        HashingWriteBuffer         original_hashing;

        MinMaxIndexPtr           minmaxes;
        MarksInCompressedFilePtr marks;
    };
    using StreamPtr     = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<ColId, StreamPtr>;

public:
    DMFileWriter(const DMFilePtr &           dmfile_,
                 const ColumnDefines &       write_columns_,
                 size_t                      min_compress_block_size_,
                 size_t                      max_compress_block_size_,
                 const CompressionSettings & compression_settings_);

    void write(const Block & block);
    void finalize();

private:
    void writeColumn(ColId col_id, const IDataType & type, const IColumn & column);
    void finalizeColumn(ColId col_id, const IDataType & type);

private:
    DMFilePtr           dmfile;
    ColumnDefines       write_columns;
    size_t              min_compress_block_size;
    size_t              max_compress_block_size;
    CompressionSettings compression_settings;

    ColumnStreams column_streams;
};

} // namespace DM
} // namespace DB