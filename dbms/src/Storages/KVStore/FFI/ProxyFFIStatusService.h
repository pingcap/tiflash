// Copyright 2025 PingCAP, Inc.
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

#include <Storages/S3/FileCache.h>

namespace DB
{

enum class EvictMethod
{
    ByFileType = 0,
    ByEvictSize,
};

struct RemoteCacheEvictRequest
{
    EvictMethod evict_method;
    FileSegment::FileType evict_type;
    size_t reserve_size = 0;
    bool force_evict = false;
    String err_msg;
};

RemoteCacheEvictRequest parseEvictRequest(std::string_view path, std::string_view api_name, std::string_view query);

} // namespace DB

template <>
struct fmt::formatter<DB::RemoteCacheEvictRequest>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::RemoteCacheEvictRequest & req, FormatContext & ctx) const
    {
        if (!req.err_msg.empty())
        {
            return fmt::format_to(ctx.out(), "{{err_msg={}}}", req.err_msg);
        }
        switch (req.evict_method)
        {
        case DB::EvictMethod::ByFileType:
            return fmt::format_to(
                ctx.out(),
                "{{method={} evict_type={}}}",
                magic_enum::enum_name(req.evict_method),
                magic_enum::enum_name(req.evict_type));
        case DB::EvictMethod::ByEvictSize:
            return fmt::format_to(
                ctx.out(),
                "{{method={} reserve_size={} force={}}}",
                magic_enum::enum_name(req.evict_method),
                req.reserve_size,
                req.force_evict);
        }
    }
};
