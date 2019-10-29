#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/TiDBBit.h>
#include <Flash/Coprocessor/TiDBDecimal.h>
#include <Flash/Coprocessor/TiDBEnum.h>
#include <Flash/Coprocessor/TiDBTime.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class TiDBColumn
{
public:
    TiDBColumn(Int8 element_len);

    void appendNull();
    void append(Int64 value);
    void append(UInt64 value);
    void append(const StringRef & value);
    void append(Float64 value);
    void append(Float32 value);
    //void appendDuration();
    void append(const TiDBTime & time);
    //void appendJson();
    void append(const TiDBDecimal & decimal);
    void append(const TiDBBit & bit);
    void append(const TiDBEnum & ti_enum);
    void encodeColumn(std::stringstream & ss);
    void clear();

private:
    bool isFixed() { return fixed_size != VAR_SIZE; };
    void finishAppendFixed();
    void finishAppendVar(UInt32 size);
    void appendNullBitMap(bool value);

    UInt32 length;
    UInt32 null_cnt;
    std::vector<UInt8> null_bitmap;
    std::vector<Int64> var_offsets;
    std::stringstream data;
    std::string default_value;
    UInt64 current_data_size;
    Int8 fixed_size;
};

} // namespace DB
