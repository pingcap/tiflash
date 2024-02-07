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

#include <Common/BitHelpers.h>
#include <Common/nocopyable.h>
#include <IO/Buffer/WriteBufferFromVector.h>
#include <common/StringRef.h>

#include <string>

namespace DB
{
/** Writes the data to a string.
  * Note: before using the resulting string, destroy this object.
  * Use 15 as initial_size to fit the local buffer of gcc's std::string.
  */
using WriteBufferFromString = WriteBufferFromVector<std::string, 15>;

namespace detail
{
/// For correct order of initialization.
class StringHolder
{
public:
    StringHolder() = default;
    StringHolder(size_t init_size) { value.resize(init_size); }

protected:
    std::string value;
};
} // namespace detail

/// Creates the string by itself and allows to get it.
class WriteBufferFromOwnString
    : public detail::StringHolder
    , public WriteBufferFromString
{
public:
    WriteBufferFromOwnString()
        : WriteBufferFromString(value)
    {}

    WriteBufferFromOwnString(size_t init_size)
        : detail::StringHolder(init_size)
        , WriteBufferFromString(value)
    {}

    DISALLOW_MOVE(WriteBufferFromOwnString);

    StringRef stringRef() const
    {
        return isFinished() ? StringRef(value) : StringRef(value.data(), pos - value.data());
    }

    std::string & str()
    {
        finalize();
        return value;
    }

    /// Can't reuse WriteBufferFromOwnString after releaseStr
    std::string releaseStr()
    {
        finalize();
        return std::move(value);
    }
};

} // namespace DB
