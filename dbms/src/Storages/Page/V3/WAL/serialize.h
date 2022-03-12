#pragma once

#include <Storages/Page/V3/PageEntriesEdit.h>

#include <string_view>

namespace DB::PS::V3::ser
{
String serializeTo(const PageEntriesEdit & edit);
PageEntriesEdit deserializeFrom(std::string_view record);

} // namespace DB::PS::V3::ser
