#pragma once

#include <string>

#include "type.h"

namespace pybind11 {
class bytes;
class str;
}  // namespace pybind11

namespace py = pybind11;

class TikvKey {
 public:
  using String = std::string;

  TikvKey() = default;
  explicit TikvKey(String && s);
  explicit TikvKey(std::string_view);
  TikvKey(const TikvKey &) = delete;
  TikvKey(TikvKey && kv);

  int Compare(const TikvKey & k);

  size_t Size() const;

  ~TikvKey();

  py::bytes ToBytes() const;

  py::str ToPdKey() const;

  const String & key() const;

 private:
  const String key_;
};

TikvKey MakeTableBegin(const TableId table_id);
TikvKey MakeTableEnd(const TableId table_id);
TikvKey MakeTableHandle(const TableId table_id, const HandleId handle);
TikvKey MakeWholeTableBegin(const TableId table_id);
TikvKey MakeWholeTableEnd(const TableId table_id);

py::bytes DecodePdKey(const char * s, const size_t len);

TableId GetTableId(const TikvKey & key);

HandleId GetHandle(const TikvKey & key);