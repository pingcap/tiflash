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

#include <Storages/Page/V3/PageEntriesEdit.h>

#include <string_view>

namespace DB::PS::V3::ser
{
String serializeTo(const PageEntriesEdit & edit);
PageEntriesEdit deserializeFrom(std::string_view record);

} // namespace DB::PS::V3::ser
