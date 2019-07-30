
#include <DataStreams/TidbCopBlockOutputStream.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/TypeMapping.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes {
    extern const int UNSUPPORTED_PARAMETER;
}

struct TypeMapping;

TidbCopBlockOutputStream::TidbCopBlockOutputStream(
    tipb::SelectResponse *response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_, Block header_)
    : response(response_), records_per_chunk(records_per_chunk_), encodeType(encodeType_), header(header_)
{
    if(encodeType == tipb::EncodeType::TypeArrow) {
        throw Exception("Encode type TypeArrow is not supported yet in TidbCopBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    current_chunk = nullptr;
    current_records_num = 0;
    total_rows = 0;
}


void TidbCopBlockOutputStream::writePrefix()
{
    //something to do here?
}

void TidbCopBlockOutputStream::writeSuffix()
{
    // error handle,
    if(current_chunk != nullptr && records_per_chunk > 0) {
        current_chunk->set_rows_data(current_ss.str());
    }
}


void TidbCopBlockOutputStream::write(const Block & block)
{
    // encode data to chunk
    size_t rows = block.rows();
    for(size_t i = 0; i < rows; i++) {
        if(current_chunk == nullptr || current_records_num >= records_per_chunk) {
            if(current_chunk) {
                // set the current ss to current chunk
                current_chunk->set_rows_data(current_ss.str());
            }
            current_chunk = response->add_chunks();
            current_ss.str("");
            records_per_chunk = 0;
        }
        for(size_t j = 0; j < block.columns(); j++) {
            auto field = (*block.getByPosition(j).column.get())[i];
            const DataTypePtr & dataTypePtr = block.getByPosition(j).type;
            if(dataTypePtr->isNullable()) {
                const DataTypePtr real = dynamic_cast<const DataTypeNullable *>(dataTypePtr.get())->getNestedType();
                EncodeDatum(field, getCodecFlagByDataType(real), current_ss);
            } else {
                EncodeDatum(field, getCodecFlagByDataType(block.getByPosition(j).type), current_ss);
            }
        }
        //encode current row
        records_per_chunk++;
        total_rows++;
    }
}

}
