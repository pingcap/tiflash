#include <Flash/Coprocessor/DAGArrowChunkBuilder.h>

namespace DB
{
void DAGArrowChunkBuilder::build(const DB::Block & block)
{
    // Encode data in chunk by array encode
    size_t rows = block.rows();
    size_t batch_encode_size;
    for (size_t row_index = 0; row_index < rows; row_index += batch_encode_size)
    {
        if (ti_chunk->getRecordSize() >= records_per_chunk)
        {
            auto * dag_chunk = dag_response.add_chunks();
            std::stringstream ss;
            ti_chunk->encodeChunk(ss);
            dag_chunk->set_rows_data(ss.str());
            dag_response.add_output_counts(ti_chunk->getRecordSize());
            ti_chunk->clear();
        }
        batch_encode_size = records_per_chunk - ti_chunk->getRecordSize();
        const size_t upper = std::min(row_index + batch_encode_size, rows);
        ti_chunk->buildDAGChunkFromBlock(block, result_field_types, row_index, upper);
    }
}

void DAGArrowChunkBuilder::buildSuffix()
{
    if (ti_chunk->getRecordSize() > 0)
    {
        auto * dag_chunk = dag_response.add_chunks();
        std::stringstream ss;
        ti_chunk->encodeChunk(ss);
        dag_chunk->set_rows_data(ss.str());
        dag_response.add_output_counts(ti_chunk->getRecordSize());
        ti_chunk->clear();
    }
}
} // namespace DB
