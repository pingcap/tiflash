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

#include <Common/nocopyable.h>

#include <string>

namespace DB
{
struct RawCppString : std::string
{
    using Base = std::string;
    using Base::Base;
    RawCppString() = delete;
    RawCppString(Base && src) // NOLINT(google-explicit-constructor)
        : Base(std::move(src))
    {}
    RawCppString(const Base & src) // NOLINT(google-explicit-constructor)
        : Base(src)
    {}
    DISALLOW_COPY(RawCppString);

    template <class... Args>
    static RawCppString * New(Args &&... _args) // NOLINT(readability-identifier-naming)
    {
        return new RawCppString{std::forward<Args>(_args)...};
    }
};

} // namespace DB
