#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/TiDBDecimal.h>
#include <Flash/Coprocessor/TiDBTime.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

const Int8 VAR_SIZE = 0;
class TiDBColumn
{
public:
    TiDBColumn(Int8 element_len, const String & default_value_);

    void appendNull();
    void appendInt64(Int64 value);
    void appendUInt64(UInt64 value);
    void appendBytes(const String & value);
    void appendBytes(const StringRef & value);
    void appendFloat64(Float64 value);
    void appendFloat32(Float32 value);
    //void appendDuration();
    void appendTime(const TiDBTime & time);
    //void appendJson();
    void appendDecimal(const TiDBDecimal & decimal);
    void encodeColumn(std::stringstream & ss);

private:
    bool isFixed() { return fixed_size != VAR_SIZE; };
    void finishAppendFixed();
    void finishAppendVar(UInt32 size);
    void appendNullBitMap(bool value);

    UInt32 length;
    UInt32 null_cnt;
    std::vector<UInt8> null_bitmap;
    std::vector<Int32> var_offsets;
    std::stringstream data;
    std::string default_value;
    UInt32 current_data_size;
    Int8 fixed_size;
};

} // namespace DB
