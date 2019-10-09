#include <Flash/Coprocessor/DAGBlockOutputStream.h>

#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/TiDBChunk.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using TiDB::DatumBumpy;
using TiDB::TP;

DAGBlockOutputStream::DAGBlockOutputStream(tipb::SelectResponse & dag_response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
    std::vector<tipb::FieldType> && result_field_types_, Block header_)
    : dag_response(dag_response_),
      records_per_chunk(records_per_chunk_),
      encodeType(encodeType_),
      result_field_types(result_field_types_),
      header(header_)
{
    //if (encodeType == tipb::EncodeType::TypeArrow)
    //{
    //    throw Exception("Encode type TypeArrow is not supported yet in DAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    //}
    current_chunk = nullptr;
    current_records_num = 0;
}


void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::writeSuffix()
{
    // error handle,
    if (current_chunk != nullptr && current_records_num > 0)
    {
        current_chunk->set_rows_data(current_ss.str());
        dag_response.add_output_counts(current_records_num);
    }
}

void DAGBlockOutputStream::encodeWithDefaultEncodeType(const Block & block)
{
    // Encode data to chunk by default encode
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; i++)
    {
        if (current_chunk == nullptr || current_records_num >= records_per_chunk)
        {
            if (current_chunk)
            {
                // set the current ss to current chunk
                current_chunk->set_rows_data(current_ss.str());
                dag_response.add_output_counts(current_records_num);
            }
            current_chunk = dag_response.add_chunks();
            current_ss.str("");
            current_records_num = 0;
        }
        for (size_t j = 0; j < block.columns(); j++)
        {
            const auto & field = (*block.getByPosition(j).column.get())[i];
            DatumBumpy datum(field, static_cast<TP>(result_field_types[j].tp()));
            EncodeDatum(datum.field(), getCodecFlagByFieldType(result_field_types[j]), current_ss);
        }
        // Encode current row
        current_records_num++;
    }
}

void DAGBlockOutputStream::encodeWithArrayEncodeType(const DB::Block & block)
{
    // Encode data in chunk by array encode
    size_t rows = block.rows();
    TiDBChunk dagChunk = TiDBChunk(result_field_types);
    dagChunk.buildDAGChunkFromBlock(block, result_field_types, 0, rows);
    std::stringstream ss;
    dagChunk.encodeChunk(ss);
    dag_response.set_row_batch_data(ss.str());
    /*
    for (size_t row_index = 0; row_index < rows; row_index += records_per_chunk)
    {
        TiDBChunk dagChunk = TiDBChunk(result_field_types);
        const size_t upper = std::min(row_index + records_per_chunk, rows);
        dagChunk.buildDAGChunkFromBlock(block, result_field_types, row_index, upper);
        auto * chunk = dag_response.add_chunks();
        std::stringstream ss;
        dagChunk.encodeChunk(ss);
        chunk->set_rows_data(ss.str());
        dag_response.add_output_counts(upper - row_index);
    }
     */
}

void DAGBlockOutputStream::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);

    // TODO: Check compatibility between field_tp_and_flags and block column types.

    if (encodeType == tipb::EncodeType::TypeDefault)
    {
        encodeWithDefaultEncodeType(block);
    }
    else
    {
        encodeWithArrayEncodeType(block);
    }
}

} // namespace DB
