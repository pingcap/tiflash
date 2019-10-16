#include <Flash/Coprocessor/DAGBlockOutputStream.h>

#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGArrowChunkBuilder.h>
#include <Flash/Coprocessor/DAGDefaultChunkBuilder.h>
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

DAGBlockOutputStream::DAGBlockOutputStream(tipb::SelectResponse & dag_response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
    std::vector<tipb::FieldType> && result_field_types_, Block header_)
    : result_field_types(result_field_types_), header(std::move(header_))
{
    if (encodeType_ == tipb::EncodeType::TypeDefault)
    {
        chunk_builder = std::make_unique<DAGDefaultChunkBuilder>(dag_response_, records_per_chunk_, result_field_types);
    }
    else if (encodeType_ == tipb::EncodeType::TypeArrow)
    {
        chunk_builder = std::make_unique<DAGArrowChunkBuilder>(dag_response_, records_per_chunk_, result_field_types);
    }
}


void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::writeSuffix()
{
    // todo error handle
    chunk_builder->buildSuffix();
}

void DAGBlockOutputStream::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);
    chunk_builder->build(block);
}

} // namespace DB
