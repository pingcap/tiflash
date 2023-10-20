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

#include <Common/Exception.h>
#include <Common/config.h>
#include <TiDB/Collation/Collator.h>
#include <TiDB/Collation/CollatorUtils.h>
#include <re2/re2.h>


#if USE_RE2_ST
#include <re2_st/re2.h>
#else
#define re2_st re2
#endif

namespace DB
{
namespace re2Util
{
re2_st::RE2::Options getDefaultRe2Options();
String getRE2ModeModifiers(const std::string & match_type, const TiDB::TiDBCollatorPtr collator = nullptr);
} // namespace re2Util
} // namespace DB
