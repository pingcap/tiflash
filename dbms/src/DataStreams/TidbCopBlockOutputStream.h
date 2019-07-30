#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{



/** Serializes the stream of blocks in tidb coprocessor format.
  * Designed for communication with tidb via coprocessor.
  */
class TidbCopBlockOutputStream : public IBlockOutputStream
{
public:
    TidbCopBlockOutputStream(
        tipb::SelectResponse *response, Int64 records_per_chunk, tipb::EncodeType encodeType, Block header);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writePrefix() override;
    void writeSuffix() override;

private:
    tipb::SelectResponse *response;
    Int64 records_per_chunk;
    tipb::EncodeType encodeType;
    Block header;
    tipb::Chunk *current_chunk;
    Int64 current_records_num;
    std::stringstream current_ss;
    Int64 total_rows;

};

}
