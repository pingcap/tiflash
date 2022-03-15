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

#include "check_region_cnt.h"

#include <pybind11/pybind11.h>

#include <numeric>

void CheckRegionCnt::Add(std::string_view s) {
  char region_id[20];
  size_t id_len = 0;

  auto p1 = s.find('\n');
  auto p2 = s.find('\n', p1 + 1);
  for (size_t i = p1 + 1; i < p2; ++i) {
    if (s[i] == ' ') {
      region_id[id_len] = 0;
      data_[std::atoll(region_id)] += 1;
      id_len = 0;
    } else {
      region_id[id_len++] = s[i];
    }
  }
}

Int64 CheckRegionCnt::Compute(UInt64 expect_cnt) {
  if (expect_cnt == 1) return data_.size();

  return std::accumulate(
      data_.begin(), data_.end(), Int64(0),
      [&](auto sum, const auto & e) { return sum + (e.second >= expect_cnt); });
}
