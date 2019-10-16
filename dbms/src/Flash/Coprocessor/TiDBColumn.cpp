#include <Flash/Coprocessor/TiDBColumn.h>

#include <Flash/Coprocessor/DAGCodec.h>
#include <IO/Endian.h>

namespace DB
{

template <typename T>
void encodeLittleEndian(const T & value, std::stringstream & ss)
{
    auto v = toLittleEndian(value);
    ss.write(reinterpret_cast<const char *>(&v), sizeof(v));
}

TiDBColumn::TiDBColumn(Int8 element_len_) : length(0), null_cnt(0), current_data_size(0), fixed_size(element_len_)
{
    if (fixed_size != VAR_SIZE)
        default_value = String(fixed_size, '0');
    var_offsets.push_back(0);
}

void TiDBColumn::clear()
{
    length = 0;
    null_cnt = 0;
    null_bitmap.clear();
    var_offsets.clear();
    var_offsets.push_back(0);
    data.str("");
    current_data_size = 0;
}

void TiDBColumn::appendNullBitMap(bool value)
{
    size_t index = length >> 3;
    if (index >= null_bitmap.size())
    {
        null_bitmap.push_back(0);
    }
    if (value)
    {
        size_t pos = length & 7;
        null_bitmap[index] |= (1 << pos);
    }
    else
    {
        null_cnt++;
    }
}

void TiDBColumn::finishAppendFixed()
{
    current_data_size += fixed_size;
    appendNullBitMap(true);
    length++;
}

void TiDBColumn::finishAppendVar(UInt32 size)
{
    current_data_size += size;
    appendNullBitMap(true);
    var_offsets.push_back(current_data_size);
    length++;
}

void TiDBColumn::appendNull()
{
    appendNullBitMap(false);
    if (isFixed())
    {
        data << default_value;
    }
    else
    {
        auto offset = var_offsets[length];
        var_offsets.push_back(offset);
    }
    length++;
}

void TiDBColumn::appendInt64(Int64 value)
{
    encodeLittleEndian<Int64>(value, data);
    finishAppendFixed();
}

void TiDBColumn::appendUInt64(UInt64 value)
{
    encodeLittleEndian<UInt64>(value, data);
    finishAppendFixed();
}

void TiDBColumn::appendTime(const TiDBTime & time)
{
    encodeLittleEndian<UInt32>(time.my_date_time.hour, data);
    encodeLittleEndian<UInt32>(time.my_date_time.micro_second, data);
    encodeLittleEndian<UInt16>(time.my_date_time.year, data);
    encodeLittleEndian<UInt8>(time.my_date_time.month, data);
    encodeLittleEndian<UInt8>(time.my_date_time.day, data);
    encodeLittleEndian<UInt8>(time.my_date_time.minute, data);
    encodeLittleEndian<UInt8>(time.my_date_time.second, data);
    // Encode an useless u16 to make byte alignment 16 bytes.
    encodeLittleEndian<UInt16>(0, data);
    encodeLittleEndian<UInt8>(time.time_type, data);
    encodeLittleEndian<Int8>(time.fsp, data);
    // Encode an useless u16 to make byte alignment 20 bytes.
    encodeLittleEndian<UInt16>(0, data);
    finishAppendFixed();
}

void TiDBColumn::appendDecimal(const TiDBDecimal & decimal)
{
    encodeLittleEndian<UInt8>(decimal.digits_int, data);
    encodeLittleEndian<UInt8>(decimal.digits_frac, data);
    encodeLittleEndian<UInt8>(decimal.result_frac, data);
    encodeLittleEndian<UInt8>((UInt8)decimal.negative, data);
    for (int i = 0; i < MAX_WORD_BUF_LEN; i++)
    {
        encodeLittleEndian<Int32>(decimal.word_buf[i], data);
    }
    finishAppendFixed();
}

void TiDBColumn::appendBytes(const StringRef & value)
{
    data.write(value.data, value.size);
    finishAppendVar(value.size);
}

void TiDBColumn::appendBytes(const DB::String & value)
{
    data << value;
    finishAppendVar(value.size());
}

void TiDBColumn::appendFloat32(DB::Float32 value)
{
    // use memcpy to avoid breaking strict-aliasing rules
    UInt32 u;
    std::memcpy(&u, &value, sizeof(value));
    encodeLittleEndian<UInt32>(u, data);
    finishAppendFixed();
}

void TiDBColumn::appendFloat64(DB::Float64 value)
{
    // use memcpy to avoid breaking strict-aliasing rules
    UInt64 u;
    std::memcpy(&u, &value, sizeof(value));
    encodeLittleEndian<UInt64>(u, data);
    finishAppendFixed();
}

void TiDBColumn::encodeColumn(std::stringstream & ss)
{
    encodeLittleEndian<UInt32>(length, ss);
    encodeLittleEndian<UInt32>(null_cnt, ss);
    if (null_cnt > 0)
    {
        for (auto c : null_bitmap)
        {
            encodeLittleEndian<UInt8>(c, ss);
        }
    }
    if (!isFixed())
    {
        for (auto c : var_offsets)
        {
            encodeLittleEndian<Int64>(c, ss);
        }
    }
    ss << data.str();
}

} // namespace DB
