// Copyright 2023 PingCAP, Inc.
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

#include <Storages/Page/PageDefinesBase.h>

namespace DB::PS::V2
{
using PageId = PageIdU64;
using PageIds = PageIdU64s;
using PageIdSet = PageIdU64Set;
static constexpr PageId INVALID_PAGE_ID = INVALID_PAGE_U64_ID;

} // namespace DB::PS::V2
