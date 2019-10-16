#include <Flash/Coprocessor/DAGDefaultChunkBuilder.h>

#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>

using TiDB::DatumBumpy;
using TiDB::TP;

namespace DB
{
void DAGDefaultChunkBuilder::build(const DB::Block & block)
{
    // TODO: Check compatibility between field_tp_and_flags and block column types.
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

void DAGDefaultChunkBuilder::buildSuffix()
{
    if (current_chunk != nullptr && current_records_num > 0)
    {
        current_chunk->set_rows_data(current_ss.str());
        dag_response.add_output_counts(current_records_num);
    }
}

} // namespace DB
