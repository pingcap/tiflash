#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <Storages/DeltaMerge/File/DMFile.h>

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
        Stream(const String & path, const DataTypePtr & type, CompressionSettings compression_settings, size_t max_compress_block_size)
            : plain_file(createWriteBufferFromFileBase(path, 0, 0, max_compress_block_size)),
              plain_hashing(*plain_file),
              compressed_buf(plain_hashing, compression_settings),
              compressed_hashing(compressed_buf),
              minmaxes(type->createColumn())
        {
        }

        void finalize()
        {
            compressed_hashing.next();
            plain_file->next();
        }

        void sync() { plain_file->sync(); }

        /// hashing -> compressed_buf -> plain_hashing -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        HashingWriteBuffer         plain_hashing;
        CompressedWriteBuffer      compressed_buf;
        HashingWriteBuffer         compressed_hashing;

        MutableColumnPtr         minmaxes;
        MarksInCompressedFilePtr marks{};
    };
    using StreamPtr     = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<ColId, StreamPtr>;

public:
    DMFileWriter(const DMFilePtr &           dmfile_,
                 const ColumnDefines &       col_defs_,
                 size_t                      min_compress_block_size_,
                 size_t                      max_compress_block_size_,
                 const CompressionSettings & compression_settings_);

    void write(const Block & block);
    void finalize();

private:
    void writeColumn(ColId col_id, const IDataType & type, const IColumn & column);

private:
    DMFilePtr           dmfile;
    ColumnDefines       col_defs;
    size_t              min_compress_block_size;
    size_t              max_compress_block_size;
    CompressionSettings compression_settings;

    ColumnStreams column_streams;

    size_t total_rows = 0;
};

} // namespace DM
} // namespace DB