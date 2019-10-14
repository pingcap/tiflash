#include <Flash/Coprocessor/DAGBlockOutputStream.h>

#include <DataTypes/DataTypeNullable.h>
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
    chunk_for_default_encode = nullptr;
    current_records_num = 0;
    dag_response.set_encode_type(encodeType);
    if (encodeType == tipb::EncodeType::TypeArrow)
        chunk_for_arrow_encode = std::make_unique<TiDBChunk>(result_field_types);
}


void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::writeSuffix()
{
    // error handle,
    if (encodeType == tipb::EncodeType::TypeDefault)
    {
        if (chunk_for_default_encode != nullptr && current_records_num > 0)
        {
            chunk_for_default_encode->set_rows_data(current_ss.str());
            dag_response.add_output_counts(current_records_num);
        }
    }
    else if (encodeType == tipb::EncodeType::TypeArrow)
    {
        if (chunk_for_arrow_encode->getRecordSize() > 0)
        {
            auto * chunk = dag_response.add_chunks();
            std::stringstream ss;
            chunk_for_arrow_encode->encodeChunk(ss);
            chunk->set_rows_data(ss.str());
            dag_response.add_output_counts(chunk_for_arrow_encode->getRecordSize());
            chunk_for_arrow_encode->clear();
        }
    }
}

void DAGBlockOutputStream::encodeWithDefaultEncodeType(const Block & block)
{
    // TODO: Check compatibility between field_tp_and_flags and block column types.
    // Encode data to chunk by default encode
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; i++)
    {
        if (chunk_for_default_encode == nullptr || current_records_num >= records_per_chunk)
        {
            if (chunk_for_default_encode)
            {
                // set the current ss to current chunk
                chunk_for_default_encode->set_rows_data(current_ss.str());
                dag_response.add_output_counts(current_records_num);
            }
            chunk_for_default_encode = dag_response.add_chunks();
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

void DAGBlockOutputStream::encodeWithArrowEncodeType(const DB::Block &block)
{
    // Encode data in chunk by array encode
    size_t rows = block.rows();
    size_t batch_encode_size;
    for (size_t row_index = 0; row_index < rows; row_index += batch_encode_size)
    {
        if (chunk_for_arrow_encode->getRecordSize() >= records_per_chunk)
        {
            auto * chunk = dag_response.add_chunks();
            std::stringstream ss;
            chunk_for_arrow_encode->encodeChunk(ss);
            chunk->set_rows_data(ss.str());
            dag_response.add_output_counts(chunk_for_arrow_encode->getRecordSize());
            chunk_for_arrow_encode->clear();
        }
        batch_encode_size = records_per_chunk - chunk_for_arrow_encode->getRecordSize();
        const size_t upper = std::min(row_index + batch_encode_size, rows);
        chunk_for_arrow_encode->buildDAGChunkFromBlock(block, result_field_types, row_index, upper);
    }
}

void DAGBlockOutputStream::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);

    if (encodeType == tipb::EncodeType::TypeDefault)
    {
        encodeWithDefaultEncodeType(block);
    }
    else
    {
        encodeWithArrowEncodeType(block);
    }
}

} // namespace DB
