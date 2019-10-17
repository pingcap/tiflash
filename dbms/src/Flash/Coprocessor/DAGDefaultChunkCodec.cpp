#include <Flash/Coprocessor/DAGDefaultChunkCodec.h>

#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>

using TiDB::DatumBumpy;
using TiDB::TP;

namespace DB
{
void DAGDefaultChunkCodec::encode(const DB::Block & block, size_t start, size_t end, std::unique_ptr<DB::DAGChunkCodecStream> & stream)
{
    // TODO: Check compatibility between field_tp_and_flags and block column types.
    auto * default_chunk_codec_stream = dynamic_cast<DB::DAGDefaultChunkCodecStream *>(stream.get());
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

Block DAGDefaultChunkCodec::decode(const tipb::Chunk &) { return {}; }
} // namespace DB
