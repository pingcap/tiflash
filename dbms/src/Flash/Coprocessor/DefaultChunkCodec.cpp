#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>

using TiDB::DatumBumpy;
using TiDB::DatumFlat;
using TiDB::TP;

namespace DB
{
class DefaultChunkCodecStream : public ChunkCodecStream
{
public:
    explicit DefaultChunkCodecStream(const std::vector<tipb::FieldType> & field_types) : ChunkCodecStream(field_types) {}
    std::stringstream ss;
    String getString() override { return ss.str(); }
    void encode(const Block & block, size_t start, size_t end) override;
    void clear() override { ss.str(""); }
};

void DefaultChunkCodecStream::encode(const Block & block, size_t start, size_t end)
{
    // TODO: Check compatibility between field_tp_and_flags and block column types.
    // Encode data to chunk by default encode
    for (size_t i = start; i < end; i++)
    {
        for (size_t j = 0; j < block.columns(); j++)
        {
            const auto & field = (*block.getByPosition(j).column.get())[i];
            DatumBumpy datum(field, static_cast<TP>(field_types[j].tp()));
            EncodeDatum(datum.field(), getCodecFlagByFieldType(field_types[j]), ss);
        }
    }
}

Block DefaultChunkCodec::decode(const tipb::Chunk & chunk, const DAGSchema & schema)
{
    std::vector<std::vector<Field>> rows;
    std::vector<Field> curr_row;
    const std::string & data = chunk.rows_data();
    size_t cursor = 0;
    while (cursor < data.size())
    {
        curr_row.push_back(DecodeDatumForCHRow(cursor, data, schema[curr_row.size()].second));
        if (curr_row.size() == schema.size())
        {
            rows.emplace_back(std::move(curr_row));
            curr_row.clear();
        }
    }

    ColumnsWithTypeAndName columns;
    for (auto & field : schema)
    {
        const auto & name = field.first;
        auto data_type = getDataTypeByColumnInfo(field.second);
        ColumnWithTypeAndName col(data_type, name);
        col.column->assumeMutable()->reserve(rows.size());
        columns.emplace_back(std::move(col));
    }
    for (const auto & row : rows)
    {
        for (size_t i = 0; i < row.size(); i++)
        {
            const Field & field = row[i];
            columns[i].column->assumeMutable()->insert(DatumFlat(field, schema[i].second.tp).field());
        }
    }
    return Block(columns);
}

std::unique_ptr<ChunkCodecStream> DefaultChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types)
{
    return std::make_unique<DefaultChunkCodecStream>(field_types);
}

} // namespace DB
