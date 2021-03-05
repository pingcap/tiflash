#pragma once

#include <string_view>
#include <unordered_map>

#include "type.h"

namespace pybind11 {
class bytes;
}  // namespace pybind11

namespace py = pybind11;

class CheckRegionCnt {
 public:
  void Add(std::string_view);
  // Count how many regions that the number of TiFlash peers is greater or equal
  // to replica_count
  Int64 Compute(UInt64);

 private:
  std::unordered_map<UInt64, UInt64> data_;
};
