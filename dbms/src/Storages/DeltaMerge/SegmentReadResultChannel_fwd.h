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

#include <common/types.h>

#include <memory>

namespace DB::DM
{

class SegmentReadResultChannel;
using SegmentReadResultChannelPtr = std::shared_ptr<SegmentReadResultChannel>;

struct SegmentReadResultChannelOptions
{
    /// The number of "sources". It should be paired when calling `finish()`.
    /// Conventionally, `source` is `{store_id}_{segment_id}`.
    const UInt64 expected_sources;

    const String & debug_tag;

    /// If we reaches this limit, we will consider this channel as full.
    /// This is not a hard limit.
    const UInt64 max_pending_blocks;

    const std::function<void()> on_first_read;
};

} // namespace DB::DM
