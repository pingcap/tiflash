#pragma once

#include <cstring>
#include <sstream>

#include "exception_util.h"
#include "type.h"

static const size_t kEncGroupSize = 8;
static const uint8_t kEncMarker = static_cast<uint8_t>(0xff);
static const char kEncAscPadding[kEncGroupSize] = {0};

static const char kTablePrefix = 't';
static const char * kRecordPrefixSep = "_r";

static const UInt64 kSignMark = (UInt64)1 << 63u;

inline void EncodeBytes(const std::string & ori_str, std::stringstream & ss) {
  const size_t len = ori_str.size();
  size_t index = 0;
  while (index <= len) {
    size_t remain = len - index;
    size_t pad = 0;
    if (remain >= kEncGroupSize) {
      ss.write(ori_str.data() + index, kEncGroupSize);
    } else {
      pad = kEncGroupSize - remain;
      ss.write(ori_str.data() + index, remain);
      ss.write(kEncAscPadding, pad);
    }
    ss.put(static_cast<char>(kEncMarker - (uint8_t)pad));
    index += kEncGroupSize;
  }
}

inline std::string Encode(const std::string & ori_str) {
  std::stringstream ss;
  EncodeBytes(ori_str, ss);
  return ss.str();
}

inline UInt64 EndianReverse(UInt64 x) {
  UInt64 step32, step16;
  step32 = x << 32u | x >> 32u;
  step16 = (step32 & 0x0000FFFF0000FFFFULL) << 16u |
           (step32 & 0xFFFF0000FFFF0000ULL) >> 16u;
  return (step16 & 0x00FF00FF00FF00FFULL) << 8u |
         (step16 & 0xFF00FF00FF00FF00ULL) >> 8u;
}

inline UInt64 ToBigEndian(const UInt64 x) { return EndianReverse(x); }

inline UInt64 EncodeUInt64(const UInt64 x) { return ToBigEndian(x); }

inline UInt64 EncodeInt64(const Int64 x) {
  return EncodeUInt64(static_cast<UInt64>(x) ^ kSignMark);
}

inline UInt64 EncodeUInt64Desc(const UInt64 x) { return EncodeUInt64(~x); }

inline UInt64 DecodeUInt64(const UInt64 x) { return ToBigEndian(x); }

inline UInt64 DecodeUInt64Desc(const UInt64 x) { return ~DecodeUInt64(x); }

inline Int64 DecodeInt64(const UInt64 x) {
  return static_cast<Int64>(DecodeUInt64(x) ^ kSignMark);
}

inline std::string GenKeyByTable(const TableId tableId) {
  std::string key(1 + 8, 0);
  memcpy(key.data(), &kTablePrefix, 1);
  auto big_endian_table_id = EncodeInt64(tableId);
  memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id),
         8);
  return Encode(key);
}

template <bool isEnd = true>
inline std::string GenKey(const TableId tableId) {
  return GenKeyByTable(tableId + 1);
}

template <>
inline std::string GenKey<false>(const TableId tableId) {
  std::string key(1 + 8 + 2, 0);
  memcpy(key.data(), &kTablePrefix, 1);
  auto big_endian_table_id = EncodeInt64(tableId);
  memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id),
         8);
  memcpy(key.data() + 1 + 8, kRecordPrefixSep, 2);
  return Encode(key);
}

inline std::string GenKey(const TableId tableId, const HandleId handle) {
  std::string key(1 + 8 + 2 + 8, 0);
  memcpy(key.data(), &kTablePrefix, 1);
  auto big_endian_table_id = EncodeInt64(tableId);
  memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id),
         8);
  memcpy(key.data() + 1 + 8, kRecordPrefixSep, 2);
  auto big_endian_handle_id = EncodeInt64(handle);
  memcpy(key.data() + 1 + 8 + 2,
         reinterpret_cast<const char *>(&big_endian_handle_id), 8);
  return Encode(key);
}

inline std::string ToPdKey(const char * key, const size_t len) {
  std::string res(len * 2, 0);
  size_t i = 0;
  for (size_t k = 0; k < len; ++k) {
    uint8_t o = key[k];
    res[i++] = o / 16;
    res[i++] = o % 16;
  }

  for (char & re : res) {
    if (re < 10)
      re = re + '0';
    else
      re = re - 10 + 'A';
  }
  return res;
}

inline std::string ToPdKey(const std::string & key) {
  return ToPdKey(key.data(), key.size());
}

inline std::string FromPdKey(const char * key, const size_t len) {
  std::string res(len / 2, 0);
  for (size_t k = 0; k < len; k += 2) {
    int s[2];

    for (size_t i = 0; i < 2; ++i) {
      char p = key[k + i];
      if (p >= 'A')
        s[i] = p - 'A' + 10;
      else
        s[i] = p - '0';
    }

    res[k / 2] = s[0] * 16 + s[1];
  }
  return res;
}

inline bool CheckKeyPaddingValid(const char * ptr, const UInt8 pad_size) {
  UInt64 p = (*reinterpret_cast<const UInt64 *>(ptr)) >>
             ((kEncGroupSize - pad_size) * 8);
  return p == 0;
}

inline std::tuple<std::string, size_t> DecodeTikvKeyFull(
    const std::string & key) {
  const size_t chunk_len = kEncGroupSize + 1;
  std::string res;
  res.reserve(key.size() / chunk_len * kEncGroupSize);
  for (const char * ptr = key.data();; ptr += chunk_len) {
    if (ptr + chunk_len > key.size() + key.data())
      throw Exception("Unexpected eof");
    auto marker = (UInt8) * (ptr + kEncGroupSize);
    UInt8 pad_size = (kEncMarker - marker);
    if (pad_size == 0) {
      res.append(ptr, kEncGroupSize);
      continue;
    }
    if (pad_size > kEncGroupSize) throw Exception("Key padding");
    res.append(ptr, kEncGroupSize - pad_size);

    if (!CheckKeyPaddingValid(ptr, pad_size))
      throw Exception("Key padding, wrong end");

    // raw string and the offset of remaining string such as timestamp
    return std::make_tuple(std::move(res), ptr - key.data() + chunk_len);
  }
}

inline std::string DecodeTikvKey(const std::string & key) {
  return std::get<0>(DecodeTikvKeyFull(key));
}
