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

#include <fmt/format.h>

#include <string>

#pragma once

namespace DB
{
namespace DM
{

struct GCOptions
{
    bool do_merge = true;
    bool do_merge_delta = true;
    bool update_safe_point = true;

    static GCOptions newAll()
    {
        return GCOptions{
            .do_merge = true,
            .do_merge_delta = true,
            .update_safe_point = true,
        };
    }

    static GCOptions newNoneForTest()
    {
        return GCOptions{
            .do_merge = false,
            .do_merge_delta = false,
            .update_safe_point = false,
        };
    }

    static GCOptions newAllForTest()
    {
        return GCOptions{
            .do_merge = true,
            .do_merge_delta = true,
            .update_safe_point = false,
        };
    }

    std::string toString() const
    {
        return fmt::format(
            "<merge={} merge_delta={} update_safe_point={}>",
            do_merge,
            do_merge_delta,
            update_safe_point);
    }
};

} // namespace DM
} // namespace DB
