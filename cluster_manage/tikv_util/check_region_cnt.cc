#include "check_region_cnt.h"
#include <pybind11/pybind11.h>

void CheckRegionCnt::Add(std::string_view s) {
  char region_id[20];
  size_t id_len = 0;

  auto p1 = s.find('\n');
  auto p2 = s.find('\n', p1 + 1);
  for (size_t i = p1 + 1; i < p2; ++i) {
    if (s[i] == ' ') {
      region_id[id_len] = 0;
      data_.emplace(std::atoll(region_id));
      id_len = 0;
    } else {
      region_id[id_len++] = s[i];
    }
  }
}

int CheckRegionCnt::Compute() { return data_.size(); }