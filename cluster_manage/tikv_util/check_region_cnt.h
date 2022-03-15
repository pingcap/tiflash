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
