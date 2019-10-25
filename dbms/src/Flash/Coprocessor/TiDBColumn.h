#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/TiDBDecimal.h>
#include <Flash/Coprocessor/TiDBTime.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

const Int8 VAR_SIZE = 0;
inline UInt8 getFieldLength(Int32 tp)
{
    switch (tp)
    {
        case TiDB::TypeTiny:
        case TiDB::TypeShort:
        case TiDB::TypeInt24:
        case TiDB::TypeLong:
        case TiDB::TypeLongLong:
        case TiDB::TypeYear:
        case TiDB::TypeDouble:
            return 8;
        case TiDB::TypeFloat:
            return 4;
        case TiDB::TypeDecimal:
        case TiDB::TypeNewDecimal:
            return 40;
        case TiDB::TypeDate:
        case TiDB::TypeDatetime:
        case TiDB::TypeNewDate:
        case TiDB::TypeTimestamp:
            return 20;
        case TiDB::TypeVarchar:
        case TiDB::TypeVarString:
        case TiDB::TypeString:
        case TiDB::TypeBlob:
        case TiDB::TypeTinyBlob:
        case TiDB::TypeMediumBlob:
        case TiDB::TypeLongBlob:
            return VAR_SIZE;
        default:
            throw Exception("not supported field type in arrow encode: " + std::to_string(tp));
    }
}

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
