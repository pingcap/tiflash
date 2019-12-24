#pragma once

#include <string_view>
#include <unordered_set>
#include "type.h"

namespace pybind11 {
class bytes;
}  // namespace pybind11

namespace py = pybind11;

class CheckRegionCnt {
 public:
  void Add(std::string_view);
  int Compute();

 private:
  std::unordered_set<UInt64> data_;
};
