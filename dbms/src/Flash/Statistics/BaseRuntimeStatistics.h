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

#include <Common/FmtUtils.h>
#include <common/types.h>

namespace DB
{
struct BlockStreamProfileInfo;
struct OperatorProfileInfo;
struct BaseRuntimeStatistics
{
    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;
    size_t allocated_bytes = 0;
    size_t concurrency = 0;
    UInt64 execution_time_ns = 0;
    UInt64 minTSO_wait_time_ns = 0;
    UInt64 queue_wait_time_ns = 0;
    UInt64 pipeline_breaker_wait_time_ns = 0;
    UInt64 inner_zone_send_bytes = 0;
    UInt64 inner_zone_receive_bytes = 0;
    UInt64 inter_zone_send_bytes = 0;
    UInt64 inter_zone_receive_bytes = 0;

    void append(const BlockStreamProfileInfo &);
    void append(const OperatorProfileInfo &);
};
} // namespace DB
