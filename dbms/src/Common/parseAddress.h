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

#include <common/types.h>

#include <map>
#include <string>


namespace DB
{
/** Parse address from string, that can contain host with or without port.
  * If port was not specified and default_port is not zero, default_port is used.
  * Otherwise, an exception is thrown.
  *
  * Examples:
  *  yandex.ru - returns "yandex.ru" and default_port
  *  yandex.ru:80 - returns "yandex.ru" and 80
  *  [2a02:6b8:a::a]:80 - returns [2a02:6b8:a::a] and 80; note that square brackets remain in returned host.
  */
std::pair<std::string, UInt16> parseAddress(const std::string & str, UInt16 default_port);

} // namespace DB
