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
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <kvproto/raft_serverpb.pb.h>
#include <raft_cmdpb.pb.h>

namespace DB
{
namespace keys
{
static constexpr uint8_t LOCAL_PREFIX = 0x1;
static constexpr uint8_t REGION_RAFT_PREFIX = 0x2;
static constexpr uint8_t REGION_META_PREFIX = 0x3;
static constexpr uint8_t REGION_STATE_SUFFIX = 0x1;
static constexpr uint8_t APPLY_STATE_SUFFIX = 0x3;

void makeRegionPrefix(WriteBuffer & ss, uint64_t region_id, uint8_t suffix);
void makeRegionMeta(WriteBuffer & ss, uint64_t region_id, uint8_t suffix);
std::string regionStateKey(uint64_t region_id);
std::string applyStateKey(uint64_t region_id);
bool validateRegionStateKey(const char * buf, size_t len, uint64_t region_id);
bool validateApplyStateKey(const char * buf, size_t len, uint64_t region_id);

} // namespace keys
} // namespace DB