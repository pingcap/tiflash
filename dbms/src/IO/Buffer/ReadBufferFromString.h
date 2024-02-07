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

#include <IO/Buffer/ReadBufferFromMemory.h>
#include <common/types.h>


namespace DB
{
/** Allows to read from std::string-like object.
  */
class ReadBufferFromString : public ReadBufferFromMemory
{
public:
    /// std::string or something similar
    template <typename S>
    explicit ReadBufferFromString(const S & s)
        : ReadBufferFromMemory(s.data(), s.size())
    {}
};

class ReadBufferFromOwnString
    : public String
    , public ReadBufferFromString
{
public:
    explicit ReadBufferFromOwnString(std::string_view s_)
        : String(s_)
        , ReadBufferFromString(*this)
    {}
};

} // namespace DB
