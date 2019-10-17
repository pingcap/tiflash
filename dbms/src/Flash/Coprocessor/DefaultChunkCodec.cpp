#include <Flash/Coprocessor/DefaultChunkCodec.h>

#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>

using TiDB::DatumBumpy;
using TiDB::DatumFlat;
using TiDB::TP;

namespace DB
{
void DefaultChunkCodec::encode(const DB::Block & block, size_t start, size_t end, const std::vector<tipb::FieldType> & result_field_types,
    std::unique_ptr<DB::ChunkCodecStream> & stream)
{
    // TODO: Check compatibility between field_tp_and_flags and block column types.
    auto * default_chunk_codec_stream = dynamic_cast<DB::DefaultChunkCodecStream *>(stream.get());
    // Encode data to chunk by default encode
    for (size_t i = start; i < end; i++)
    {
        for (size_t j = 0; j < block.columns(); j++)
        {
            const auto & field = (*block.getByPosition(j).column.get())[i];
            DatumBumpy datum(field, static_cast<TP>(result_field_types[j].tp()));
            EncodeDatum(datum.field(), getCodecFlagByFieldType(result_field_types[j]), default_chunk_codec_stream->ss);
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
        curr_row.push_back(DecodeDatum(cursor, data));
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

} // namespace DB
