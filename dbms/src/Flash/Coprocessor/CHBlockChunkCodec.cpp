#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

class CHBlockChunkCodecStream : public ChunkCodecStream
{
public:
    explicit CHBlockChunkCodecStream(const std::vector<tipb::FieldType> & field_types) : ChunkCodecStream(field_types)
    {
        output = std::make_unique<WriteBufferFromOwnString>();
    }

    String getString() override
    {
        std::stringstream ss;
        return output->str();
    }
    void clear() override { output = std::make_unique<WriteBufferFromOwnString>(); }
    void encode(const Block & block, size_t start, size_t end) override;
    std::unique_ptr<WriteBufferFromOwnString> output;
};

void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column;

    if (ColumnPtr converted = column->convertToFullColumnIfConst())
        full_column = converted;
    else
        full_column = column;

    IDataType::OutputStreamGetter output_stream_getter = [&](const IDataType::SubstreamPath &) { return &ostr; };
    type.serializeBinaryBulkWithMultipleStreams(*full_column, output_stream_getter, offset, limit, false, {});
}

void CHBlockChunkCodecStream::encode(const Block & block, size_t start, size_t end)
{
    // Encode data in chunk by chblock encode
    if (start != 0 || end != block.rows())
        throw Exception("CHBlock encode only support encode whole block");
    block.checkNumberOfRows();
    size_t columns = block.columns();
    size_t rows = block.rows();

    writeVarUInt(columns, *output);
    writeVarUInt(rows, *output);

    for (size_t i = 0; i < columns; i++)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);

        writeStringBinary(column.name, *output);
        writeStringBinary(column.type->getName(), *output);

        if (rows)
            writeData(*column.type, column.column, *output, 0, 0);
    }
}

std::unique_ptr<ChunkCodecStream> CHBlockChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types)
{
    return std::make_unique<CHBlockChunkCodecStream>(field_types);
}

Block CHBlockChunkCodec::decode(const tipb::Chunk & chunk, const DAGSchema &)
{
    const String & row_data = chunk.rows_data();
    ReadBufferFromString read_buffer(row_data);
    NativeBlockInputStream block_in(read_buffer, 0);
    return block_in.read();
}

} // namespace DB
