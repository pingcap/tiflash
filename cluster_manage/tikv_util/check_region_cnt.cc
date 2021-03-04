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
