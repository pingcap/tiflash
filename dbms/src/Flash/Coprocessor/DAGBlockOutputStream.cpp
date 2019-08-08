#include <Flash/Coprocessor/DAGBlockOutputStream.h>

#include <DataTypes/DataTypeNullable.h>
#include <Storages/Transaction/Codec.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

DAGBlockOutputStream::DAGBlockOutputStream(tipb::SelectResponse & dag_response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
    FieldTpAndFlags && field_tp_and_flags_, Block header_)
    : dag_response(dag_response_),
      records_per_chunk(records_per_chunk_),
      encodeType(encodeType_),
      field_tp_and_flags(field_tp_and_flags_),
      header(header_)
{
    if (encodeType == tipb::EncodeType::TypeArrow)
    {
        throw Exception("Encode type TypeArrow is not supported yet in DAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    current_chunk = nullptr;
    current_records_num = 0;
    total_rows = 0;
}


void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::writeSuffix()
{
    // error handle,
    if (current_chunk != nullptr && records_per_chunk > 0)
    {
        current_chunk->set_rows_data(current_ss.str());
    }
}

void DAGBlockOutputStream::write(const Block & block)
{
    if (block.columns() != field_tp_and_flags.size())
        throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);

    // TODO: Check compatibility between field_tp_and_flags and block column types.

    // Encode data to chunk
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; i++)
    {
        if (current_chunk == nullptr || current_records_num >= records_per_chunk)
        {
            if (current_chunk)
            {
                // set the current ss to current chunk
                current_chunk->set_rows_data(current_ss.str());
            }
            current_chunk = dag_response.add_chunks();
            current_ss.str("");
            records_per_chunk = 0;
        }
        for (size_t j = 0; j < block.columns(); j++)
        {
            auto field = (*block.getByPosition(j).column.get())[i];
            EncodeDatum(field, field_tp_and_flags[j].getCodecFlag(), current_ss);
        }
        // Encode current row
        records_per_chunk++;
        total_rows++;
    }
}

} // namespace DB
