#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
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

    using ColumnMinMaxIndexs = std::map<String, MinMaxIndexPtr>;

public:
    DMFileWriter(const DMFilePtr &           dmfile_,
                 const ColumnDefines &       write_columns_,
                 size_t                      min_compress_block_size_,
                 size_t                      max_compress_block_size_,
                 const CompressionSettings & compression_settings_,
                 const FileProviderPtr &     file_provider_);

    void write(const Block & block, size_t not_clean_rows);
    void finalize();

private:
    void writeColumn(ColId col_id, const IDataType & type);
    void finalizeColumn(ColId col_id, const IDataType & type);

private:
    DMFilePtr           dmfile;
    ColumnDefines       write_columns;
    size_t              min_compress_block_size;
    size_t              max_compress_block_size;
    CompressionSettings compression_settings;

    ColumnMinMaxIndexs minmaxindexs;

    using Blocks = std::vector<Block>;
    Blocks blocks;

    FileProviderPtr file_provider;

    /// compressed_buf -> plain_file
    WriteBufferFromFileBasePtr plain_file;
    CompressedWriteBuffer      compressed_buf;
};

} // namespace DM
} // namespace DB
