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
        Stream(const DMFilePtr &   dmfile,
               const String &      file_base_name,
               const DataTypePtr & type,
               CompressionSettings compression_settings,
               size_t              max_compress_block_size,
               bool                do_index)
            : plain_file(createWriteBufferFromFileBase(dmfile->colDataPath(file_base_name), 0, 0, max_compress_block_size)),
              plain_hashing(*plain_file),
              compressed_buf(plain_hashing, compression_settings),
              original_hashing(compressed_buf),
              minmaxes(do_index ? std::make_shared<MinMaxIndex>(*type) : nullptr),
              mark_file(dmfile->colMarkPath(file_base_name))
        {
        }

        void flush()
        {
            original_hashing.next();
            compressed_buf.next();
            plain_hashing.next();
            plain_file->next();

            plain_file->sync();
            mark_file.sync();
        }

        /// original_hashing -> compressed_buf -> plain_hashing -> plain_file
        WriteBufferFromFileBasePtr plain_file;
        HashingWriteBuffer         plain_hashing;
        CompressedWriteBuffer      compressed_buf;
        HashingWriteBuffer         original_hashing;

        MinMaxIndexPtr      minmaxes;
        WriteBufferFromFile mark_file;
    };
    using StreamPtr     = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

public:
    DMFileWriter(const DMFilePtr &           dmfile_,
                 const ColumnDefines &       write_columns_,
                 size_t                      min_compress_block_size_,
                 size_t                      max_compress_block_size_,
                 const CompressionSettings & compression_settings_,
                 bool                        wal_mode_ = false);

    void write(const Block & block, size_t not_clean_rows);
    void finalize();

private:
    void writeColumn(ColId col_id, const IDataType & type, const IColumn & column);
    void finalizeColumn(ColId col_id, const IDataType & type);

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
    bool                wal_mode;

    ColumnStreams       column_streams;
    WriteBufferFromFile chunk_stat_file;
};

} // namespace DM
} // namespace DB