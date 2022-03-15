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

#include <pybind11/pybind11.h>

#include "check_region_cnt.h"
#include "tikv_key.h"
namespace py = pybind11;

PYBIND11_MODULE(common, m) {
  py::class_<TikvKey>(m, "TikvKey")
      .def(py::init<std::string_view>())
      .def("size", &TikvKey::Size)
      .def("compare", &TikvKey::Compare)
      .def("to_bytes", &TikvKey::ToBytes)
      .def("to_pd_key", &TikvKey::ToPdKey);

  py::class_<CheckRegionCnt>(m, "CheckRegionCnt")
      .def(py::init())
      .def("add", &CheckRegionCnt::Add)
      .def("compute", &CheckRegionCnt::Compute);

  m.def("make_table_begin", &MakeTableBegin);
  m.def("make_table_end", &MakeTableEnd);
  m.def("make_whole_table_begin", &MakeWholeTableBegin);
  m.def("make_whole_table_end", &MakeWholeTableEnd);
  m.def("make_table_handle", &MakeTableHandle);
  m.def("decode_pd_key", &DecodePdKey);
  m.def("get_table_id", &GetTableId);
  m.def("get_handle", &GetHandle);
}
