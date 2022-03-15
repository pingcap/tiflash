// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tikv_key.h"

#include <pybind11/pybind11.h>

#include "codec.h"
namespace py = pybind11;

TikvKey::TikvKey(String && s) : key_(std::move(s)) {}
TikvKey::TikvKey(std::string_view s) : key_(s) {}

TikvKey::TikvKey(TikvKey && kv) : key_(kv.key_) {}

int TikvKey::Compare(const TikvKey & k) { return key().compare(k.key()); }

size_t TikvKey::Size() const { return key().size(); }

TikvKey::~TikvKey() {}

py::bytes TikvKey::ToBytes() const { return key(); }

py::str TikvKey::ToPdKey() const { return ::ToPdKey(key_); }

const TikvKey::String & TikvKey::key() const { return key_; }

py::bytes DecodePdKey(const char * s, const size_t len) {
  return FromPdKey(s, len);
}

TikvKey MakeTableBegin(const TableId table_id) {
  return TikvKey(GenKey<false>(table_id));
}

TikvKey MakeTableEnd(const TableId table_id) {
  return TikvKey(GenKey<true>(table_id));
}

TikvKey MakeWholeTableBegin(const TableId table_id) {
  return TikvKey(GenKeyByTable(table_id));
}

TikvKey MakeWholeTableEnd(const TableId table_id) {
  return TikvKey(GenKeyByTable(table_id + 1));
}

TikvKey MakeTableHandle(const TableId table_id, const HandleId handle) {
  return TikvKey(GenKey(table_id, handle));
}

template <typename T>
inline T Read(const char * s) {
  return *(reinterpret_cast<const T *>(s));
}

TableId GetTableIdRaw(const std::string & key) {
  return DecodeInt64(Read<UInt64>(key.data() + 1));
}

HandleId GetHandleRaw(const std::string & key) {
  return DecodeInt64(Read<UInt64>(key.data() + 1 + 8 + 2));
}

TableId GetTableId(const TikvKey & key) {
  return GetTableIdRaw(DecodeTikvKey(key.key()));
}

HandleId GetHandle(const TikvKey & key) {
  return GetHandleRaw(DecodeTikvKey(key.key()));
}
