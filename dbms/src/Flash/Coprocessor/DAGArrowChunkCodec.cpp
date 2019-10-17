#include <Flash/Coprocessor/DAGArrowChunkCodec.h>

namespace DB
{
void DAGArrowChunkCodec::encode(const DB::Block & block, size_t start, size_t end, std::unique_ptr<DB::DAGChunkCodecStream> & stream)
{
    // Encode data in chunk by array encode
    auto * arrow_chunk_codec_stream = dynamic_cast<DB::DAGArrowChunkCodecStream *>(stream.get());
    arrow_chunk_codec_stream->ti_chunk->buildDAGChunkFromBlock(block, result_field_types, start, end);
}

Block DAGArrowChunkCodec::decode(const tipb::Chunk &) { return {}; }

} // namespace DB
