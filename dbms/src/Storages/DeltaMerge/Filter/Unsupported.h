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

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{
namespace DM
{
class Unsupported : public RSOperator
{
    String content;
    String reason;
    bool is_not;

public:
    Unsupported(const String & content_, const String & reason_)
        : Unsupported(content_, reason_, false)
    {}
    Unsupported(const String & content_, const String & reason_, bool is_not_)
        : content(content_)
        , reason(reason_)
        , is_not(is_not_)
    {}

    String name() override { return "unsupported"; }

    Attrs getAttrs() override { return {}; }

    String toDebugString() override
    {
        return R"({"op":")" + name() + //
            R"(","reason":")" + reason + //
            R"(","content":")" + content + //
            R"(","is_not":")" + DB::toString(is_not) + "\"}";
    }

    RSResult roughCheck(size_t /*pack_id*/, const RSCheckParam & /*param*/) override { return Some; }
};

} // namespace DM

} // namespace DB
