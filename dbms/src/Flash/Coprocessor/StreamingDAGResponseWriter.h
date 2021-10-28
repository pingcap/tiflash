#pragma once

#include <Common/LogWithPrefix.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <common/ThreadPool.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
/// Serializes the stream of blocks and sends them to TiDB or TiFlash with different serialization paths.
/// When sending data to TiDB, blocks with extra info are written into tipb::SelectResponse, then the whole tipb::SelectResponse is further serialized into mpp::MPPDataPacket.data.
/// Differently when sending data to TiFlash, blocks with only tuples are directly serialized into mpp::MPPDataPacket.chunks, but for the last block, its extra info (like execution summaries) is written into tipb::SelectResponse, then further serialized into mpp::MPPDataPacket.data.
template <class StreamWriterPtr>
class StreamingDAGResponseWriter : public DAGResponseWriter
{
public:
    StreamingDAGResponseWriter(
        StreamWriterPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        tipb::ExchangeType exchange_type_,
        Int64 records_per_chunk_,
        Int64 batch_send_min_limit_,
        bool for_last_response,
        tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types,
        DAGContext & dag_context_,
        const LogWithPrefixPtr & log_);
    void write(const Block & block) override;
    void finishWrite() override;

private:
    template <bool for_last_response>
    void batchWrite();
    template <bool for_last_response>
    void encodeThenWriteBlocks(const std::vector<Block> & input_blocks, tipb::SelectResponse & response) const;
    template <bool for_last_response>
    void partitionAndEncodeThenWriteBlocks(std::vector<Block> & input_blocks, tipb::SelectResponse & response) const;

    Int64 batch_send_min_limit;
    bool for_last_response; /// only one stream needs to sending execution summaries.
    tipb::ExchangeType exchange_type;
    StreamWriterPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks;
    uint16_t partition_num;
    LogWithPrefixPtr log;
};

} // namespace DB
