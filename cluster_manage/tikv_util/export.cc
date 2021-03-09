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
