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
#include <common/getMemoryAmount.h>
#include <common/types.h>

#include <thread>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class ServerInfo
{
public:
    struct CPUInfo
    {
        /// number of logical CPU cores
        UInt16 logical_cores = std::thread::hardware_concurrency();
        /// number of physical CPU cores
        UInt16 physical_cores = std::thread::hardware_concurrency() / 2;
        /// number of L1 cache size
        /// units: Byte
        UInt32 l1_cache_size = 16384; // 16KB (typical value)
        /// number of L2 cache size
        /// units: Byte
        UInt32 l2_cache_size = 65536; // 64KB (typical value)
        /// number of L3 cache size
        /// units: Byte
        UInt32 l3_cache_size = 2097152; // 2MB (typical value)
        /// number of L1 cache line size
        UInt8 l1_cache_line_size = 64; // 64B (typical value)
        /// number of L2 cache line size
        UInt8 l2_cache_line_size = 64; // 64B (typical value)
        /// number of L3 cache line size
        UInt8 l3_cache_line_size = 64; // 64B (typical value)
        /// CPU architecture
        String arch;
        /// CPU frequency
        String frequency;
    };

    struct Disk
    {
        String name;
        enum DiskType
        {
            UNKNOWN = 0,
            HDD = 1,
            SSD = 2
        };
        DiskType disk_type;
        UInt64 total_space = 0;
        UInt64 free_space = 0;
        String mount_point;
        String fs_type;
    };
    using DiskInfo = std::vector<Disk>;

    struct MemoryInfo
    {
        /// total memory size
        /// units: Byte
        UInt64 capacity = getMemoryAmount();
    };

    ServerInfo() = default;
    ~ServerInfo() = default;
    void parseCPUInfo(const diagnosticspb::ServerInfoItem & cpu_info_item);
    void parseDiskInfo(const diagnosticspb::ServerInfoItem & disk_info_item);
    void parseMemoryInfo(const diagnosticspb::ServerInfoItem & memory_info_item);
    void parseSysInfo(const diagnosticspb::ServerInfoResponse & sys_info_response);
    String debugString() const;

    CPUInfo cpu_info;
    DiskInfo disk_infos;
    MemoryInfo memory_info;
};
} // namespace DB
