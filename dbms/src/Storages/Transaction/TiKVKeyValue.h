#pragma once

#include <sstream>

#include <common/likely.h>

#include <Common/Exception.h>
#include <Core/Types.h>
#include <IO/Endian.h>

#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/SerializationHelper.h>
#include <Storages/Transaction/TiKVVarInt.h>
#include <Storages/Transaction/Types.h>


namespace DB
{

namespace ErrorCodes
{
// REVIEW: TIKV_ENCODE/DECODE_ERROR ?
extern const int LOGICAL_ERROR;
}

template <bool is_key>
struct StringObject
{
public:
    using T = StringObject<is_key>;

    struct Hash
    {
        std::size_t operator()(const T & x) const { return std::hash<String>()(x.str); }
    };

    StringObject() = default;
    explicit StringObject(String && str_) : str(std::move(str_)) {}
    explicit StringObject(const String & str_) : str(str_) {}
    StringObject(StringObject && obj) : str(std::move(obj.str)) {}
    StringObject(const StringObject & obj) : str(obj.str) {}
    StringObject & operator=(const StringObject & a)
    {
        str = a.str;
        return *this;
    }
    StringObject & operator=(StringObject && a)
    {
        str = std::move(a.str);
        return *this;
    }

    const String & getStr() const { return str; }
    String & getStrRef() { return str; }
    const char * data() const { return str.data(); }
    size_t dataSize() const { return str.size(); }

    String toString() const { return str; }

    // For debug
    String toHex() const
    {
        std::stringstream ss;
        ss << str.size() << "[" << std::hex;
        for (size_t i = 0; i < str.size(); ++i)
            ss << str.at(i) << ((i + 1 == str.size()) ? "" : " ");
        ss << "]";
        return ss.str();
    }

    bool empty() const { return str.empty(); }
    explicit operator bool() const { return !str.empty(); }
    bool operator==(const T & rhs) const { return str == rhs.str; }
    bool operator!=(const T & rhs) const { return str != rhs.str; }
    bool operator<(const T & rhs) const { return str < rhs.str; }
    bool operator<=(const T & rhs) const { return str <= rhs.str; }
    bool operator>(const T & rhs) const { return str > rhs.str; }
    bool operator>=(const T & rhs) const { return str >= rhs.str; }

    size_t serialize(WriteBuffer & buf) const { return writeBinary2(str, buf); }

    static T deserialize(ReadBuffer & buf) { return T(readBinary2<String>(buf)); }

private:
    String str;
};

using TiKVKey = StringObject<true>;
using TiKVValue = StringObject<false>;
using TiKVKeyValue = std::pair<TiKVKey, TiKVValue>;


namespace RecordKVFormat
{

static const char TABLE_PREFIX = 't';
static const char * RECORD_PREFIX_SEP = "_r";
static const char SHORT_VALUE_PREFIX = 'v';

static const size_t SHORT_VALUE_MAX_LEN = 64;

static const UInt64 SIGN_MARK = 0x8000000000000000;

static const size_t RAW_KEY_NO_HANDLE_SIZE = 1 + 8 + 2;
static const size_t RAW_KEY_SIZE = RAW_KEY_NO_HANDLE_SIZE + 8;


inline TiKVKeyValue genKV(const raft_cmdpb::PutRequest & req) { return {TiKVKey(req.key()), TiKVValue(req.value())}; }

inline TiKVKey genKey(const raft_cmdpb::GetRequest & req) { return TiKVKey{req.key()}; }

inline TiKVKey genKey(const raft_cmdpb::DeleteRequest & req) { return TiKVKey{req.key()}; }

inline std::vector<Field> DecodeRow(const TiKVValue & value)
{
    std::vector<Field> vec;
    const String & raw_value = value.getStr();
    size_t cursor = 0;
    while (cursor < raw_value.size())
    {
        vec.push_back(DecodeDatum(cursor, raw_value));
    }
    return vec;
}

// Key format is here:
// https://docs.google.com/document/d/1J9Dsp8l5Sbvzjth77hK8yx3SzpEJ4SXaR_wIvswRhro/edit
// https://github.com/tikv/tikv/blob/289ce2ddac505d7883ec616c078e184c00844d17/src/util/codec/bytes.rs#L33-L63
inline void encodeAsTiKVKey(const String & ori_str, std::stringstream & ss) { EncodeBytes(ori_str, ss); }

inline TiKVKey encodeAsTiKVKey(const String & ori_str)
{
    std::stringstream ss;
    encodeAsTiKVKey(ori_str, ss);
    return TiKVKey(ss.str());
}

inline UInt64 encodeUInt64(const UInt64 x) { return toBigEndian(x); }

inline UInt64 encodeInt64(const Int64 x) { return encodeUInt64(static_cast<UInt64>(x) ^ SIGN_MARK); }

inline UInt64 encodeUInt64Desc(const UInt64 x) { return encodeUInt64(~x); }

inline UInt64 decodeUInt64(const UInt64 x) { return toBigEndian(x); }

inline UInt64 decodeUInt64Desc(const UInt64 x) { return ~decodeUInt64(x); }

inline Int64 decodeInt64(const UInt64 x) { return static_cast<Int64>(decodeUInt64(x) ^ SIGN_MARK); }

inline TiKVValue EncodeRow(const TiDB::TableInfo & table_info, const std::vector<Field> & fields)
{
    if (table_info.columns.size() != fields.size())
        throw Exception("Encoding row has different sizes between columns and values", ErrorCodes::LOGICAL_ERROR);
    std::stringstream ss;
    for (size_t i = 0; i < fields.size(); i++)
    {
        const TiDB::ColumnInfo & column = table_info.columns[i];
        EncodeDatum(Field(column.id), TiDB::CodecFlagInt, ss);
        EncodeDatum(fields[i], column.getCodecFlag(), ss);
    }
    return TiKVValue(ss.str());
}

template <typename T>
inline T read(const char * s)
{
    return *(reinterpret_cast<const T *>(s));
}

inline String genRawKey(const TableID tableId, const HandleID handleId)
{
    std::stringstream key;
    key.put(RecordKVFormat::TABLE_PREFIX);
    auto big_endian_table_id = encodeInt64(tableId);
    key.write(reinterpret_cast<const char *>(&big_endian_table_id), sizeof(big_endian_table_id));
    key.write(RecordKVFormat::RECORD_PREFIX_SEP, 2);
    auto big_endian_handle_id = encodeInt64(handleId);
    key.write(reinterpret_cast<const char *>(&big_endian_handle_id), sizeof(big_endian_handle_id));
    return key.str();
}

inline TiKVKey genKey(const TableID tableId, const HandleID handleId) { return encodeAsTiKVKey(genRawKey(tableId, handleId)); }

inline std::tuple<String, size_t> decodeTiKVKeyFull(const TiKVKey & key)
{
    std::stringstream res;
    const char * ptr = key.data();
    const size_t chunk_len = ENC_GROUP_SIZE + 1;
    for (const char * next_ptr = ptr;; next_ptr += chunk_len)
    {
        ptr = next_ptr;
        if (ptr + chunk_len > key.dataSize() + key.data())
            throw Exception("Unexpected eof", ErrorCodes::LOGICAL_ERROR);
        auto marker = (UInt8) * (ptr + ENC_GROUP_SIZE);
        size_t pad_size = (ENC_MARKER - marker);
        if (pad_size == 0)
        {
            res.write(ptr, ENC_GROUP_SIZE);
            continue;
        }
        if (pad_size > ENC_GROUP_SIZE)
            throw Exception("Key padding", ErrorCodes::LOGICAL_ERROR);
        res.write(ptr, ENC_GROUP_SIZE - pad_size);
        for (const char * p = ptr + ENC_GROUP_SIZE - pad_size; p < ptr + ENC_GROUP_SIZE; ++p)
        {
            if (*p != 0)
                throw Exception("Key padding", ErrorCodes::LOGICAL_ERROR);
        }
        // raw string and the offset of remaining string such as timestamp
        return std::make_tuple(res.str(), ptr - key.data() + chunk_len);
    }
}

inline String decodeTiKVKey(const TiKVKey & key) { return std::get<0>(decodeTiKVKeyFull(key)); }

inline Timestamp getTs(const TiKVKey & key) { return decodeUInt64Desc(read<UInt64>(key.data() + key.dataSize() - 8)); }

inline TableID getTableId(const String & key) { return decodeInt64(read<UInt64>(key.data() + 1)); }

inline HandleID getHandle(const String & key) { return decodeInt64(read<UInt64>(key.data() + RAW_KEY_NO_HANDLE_SIZE)); }

inline TableID getTableId(const TiKVKey & key) { return getTableId(decodeTiKVKey(key)); }

inline HandleID getHandle(const TiKVKey & key) { return getHandle(decodeTiKVKey(key)); }

inline bool isRecord(const String & raw_key)
{
    return raw_key.size() >= RAW_KEY_SIZE && raw_key[0] == TABLE_PREFIX && memcmp(raw_key.data() + 9, RECORD_PREFIX_SEP, 2) == 0;
}

inline TiKVKey truncateTs(const TiKVKey & key) { return TiKVKey(String(key.data(), key.dataSize() - sizeof(Timestamp))); }

inline TiKVKey appendTs(const TiKVKey & key, Timestamp ts)
{
    auto big_endian_ts = encodeUInt64Desc(ts);
    auto str = key.getStr();
    return TiKVKey(str.append(reinterpret_cast<const char *>(&big_endian_ts), sizeof(big_endian_ts)));
}

inline void changeTs(TiKVKey & key, Timestamp ts)
{
    auto big_endian_ts = encodeUInt64Desc(ts);
    auto str = key.getStrRef();
    *(reinterpret_cast<Timestamp *>(str.data() + str.size() - sizeof(Timestamp))) = big_endian_ts;
}

inline TiKVKey genKey(TableID tableId, HandleID handleId, Timestamp ts)
{
    TiKVKey key = genKey(tableId, handleId);
    return appendTs(key, ts);
}

inline TiKVValue internalEncodeLockCfValue(UInt8 lock_type, const String & primary, Timestamp ts, UInt64 ttl, const String * short_value)
{
    std::stringstream res;
    res.put(lock_type);
    TiKV::writeVarInt(static_cast<Int64>(primary.size()), res);
    res.write(primary.data(), primary.size());
    TiKV::writeVarUInt(ts, res);
    TiKV::writeVarUInt(ttl, res);
    if (short_value)
    {
        res.put(SHORT_VALUE_PREFIX);
        res.put(static_cast<char>(short_value->size()));
        res.write(short_value->data(), short_value->size());
    }
    return TiKVValue(res.str());
}


inline TiKVValue encodeLockCfValue(UInt8 lock_type, const String & primary, Timestamp ts, UInt64 ttl, const String & short_value)
{
    return internalEncodeLockCfValue(lock_type, primary, ts, ttl, &short_value);
}


inline TiKVValue encodeLockCfValue(UInt8 lock_type, const String & primary, Timestamp ts, UInt64 ttl)
{
    return internalEncodeLockCfValue(lock_type, primary, ts, ttl, nullptr);
}

using DecodedLockCFValue = std::tuple<UInt8, String, Timestamp, UInt64, std::unique_ptr<String>>;

inline std::tuple<UInt8, String, UInt64, UInt64, std::unique_ptr<String>> decodeLockCfValue(const TiKVValue & value)
{
    UInt8 lock_type;
    String primary;
    UInt64 ts;
    UInt64 ttl = 0;

    const char * data = value.data();
    size_t len = value.dataSize();
    lock_type = static_cast<UInt8>(*data);
    data += 1, len -= 1; //lock type
    Int64 primary_len = 0;
    auto cur = TiKV::readVarInt(primary_len, data, len); // primary
    len -= cur - data, data = cur;
    primary.append(data, static_cast<size_t>(primary_len));
    len -= primary_len, data += primary_len;
    cur = TiKV::readVarUInt(ts, data, len); // ts
    len -= cur - data, data = cur;
    if (len == 0)
        return std::make_tuple(lock_type, primary, ts, ttl, nullptr);
    cur = TiKV::readVarUInt(ttl, data, len); // ttl
    len -= cur - data, data = cur;
    if (len == 0)
        return std::make_tuple(lock_type, primary, ts, ttl, nullptr);
    char flag = *data;
    data += 1, len -= 1; //  SHORT_VALUE_PREFIX
    assert(flag == SHORT_VALUE_PREFIX);
    (void)flag;
    auto slen = (size_t)*data;
    data += 1, len -= 1;
    assert(len == slen);
    (void)slen;
    return std::make_tuple(lock_type, primary, ts, ttl, std::make_unique<String>(data, len));
}

using DecodedWriteCFValue = std::tuple<UInt8, Timestamp, std::unique_ptr<String>>;

inline DecodedWriteCFValue decodeWriteCfValue(const TiKVValue & value)
{
    const char * data = value.data();
    size_t len = value.dataSize();

    auto write_type = static_cast<UInt8>(*data);
    data += 1, len -= 1; //write type

    Timestamp ts;
    const char * res = TiKV::readVarUInt(ts, data, len);
    len -= res - data, data = res; // ts

    if (len == 0)
        return std::make_tuple(write_type, ts, nullptr);
    assert(*data == SHORT_VALUE_PREFIX);
    data += 1, len -= 1;
    auto slen = (size_t)*data;
    data += 1, len -= 1;
    if (slen != len)
        throw Exception("unexpected eof.", ErrorCodes::LOGICAL_ERROR);
    return std::make_tuple(write_type, ts, std::make_unique<String>(data, len));
}


inline TiKVValue internalEncodeWriteCfValue(UInt8 write_type, Timestamp ts, const String * short_value)
{
    std::stringstream res;
    res.put(write_type);
    TiKV::writeVarUInt(ts, res);
    if (short_value)
    {
        res.put(SHORT_VALUE_PREFIX);
        res.put(static_cast<char>(short_value->size()));
        res.write(short_value->data(), short_value->size());
    }
    return TiKVValue(res.str());
}


inline TiKVValue encodeWriteCfValue(UInt8 write_type, Timestamp ts, const String & short_value)
{
    return internalEncodeWriteCfValue(write_type, ts, &short_value);
}


inline TiKVValue encodeWriteCfValue(UInt8 write_type, Timestamp ts) { return internalEncodeWriteCfValue(write_type, ts, nullptr); }

} // namespace RecordKVFormat

namespace TiKVRange
{

template <bool start, bool decoded = false>
inline HandleID getRangeHandle(const TiKVKey & tikv_key, const TableID table_id)
{
    constexpr HandleID min = std::numeric_limits<HandleID>::min();
    constexpr HandleID max = std::numeric_limits<HandleID>::max();

    if (tikv_key.empty())
    {
        if constexpr (start)
            return min;
        else
            return max;
    }

    String key;
    if constexpr (decoded)
        key = tikv_key.getStr();
    else
        key = RecordKVFormat::decodeTiKVKey(tikv_key);

    if (key <= RecordKVFormat::genRawKey(table_id, min))
        return min;
    if (key >= RecordKVFormat::genRawKey(table_id, max))
        return max;

    if (key.size() < RecordKVFormat::RAW_KEY_SIZE)
    {
        UInt64 tmp = 0;
        memcpy(&tmp, key.data() + RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, key.size() - RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE);
        HandleID res = RecordKVFormat::decodeInt64(tmp);
        return res;
    }

    if (key.size() > RecordKVFormat::RAW_KEY_SIZE)
    {
        HandleID res = RecordKVFormat::getHandle(key);
        return res + 1;
    }

    return RecordKVFormat::getHandle(key);
}

inline bool checkTableInvolveRange(const TableID table_id, const std::pair<TiKVKey, TiKVKey> & range)
{
    const TiKVKey start_key = RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::min());
    const TiKVKey end_key = RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::max());
    if (end_key < range.first || (!range.second.empty() && start_key >= range.second))
        return false;
    return true;
}

} // namespace TiKVRange

} // namespace DB
