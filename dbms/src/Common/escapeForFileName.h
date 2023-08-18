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

#include <common/StringRef.h>

#include <string>


namespace DB
{
/** Convert a string, so result could be used as a file name.
  * In fact it percent-encode all non-word characters, as in URL.
  */

std::string escapeForFileName(const StringRef & s);
std::string unescapeForFileName(const StringRef & s);
} // namespace DB
