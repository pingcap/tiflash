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

#include <Common/FmtUtils.h>
#include <Server/ServerInfo.h>

#include <unordered_map>

namespace DB
{
using diagnosticspb::ServerInfoItem;
using diagnosticspb::ServerInfoResponse;

void ServerInfo::parseCPUInfo(const diagnosticspb::ServerInfoItem & cpu_info_item)
{
    for (const auto & pair : cpu_info_item.pairs())
    {
        const auto & key = pair.key();
        if (key == "cpu-logical-cores")
        {
            cpu_info.logical_cores = static_cast<UInt16>(std::stoi(pair.value()));
        }
        else if (key == "cpu-physical-cores")
        {
            cpu_info.physical_cores = static_cast<UInt16>(std::stoi(pair.value()));
        }
        else if (key == "cpu-frequency")
        {
            cpu_info.frequency = pair.value();
        }
        else if (key == "l1-cache-size")
        {
            cpu_info.l1_cache_size = static_cast<UInt32>(std::stoull(pair.value()));
        }
        else if (key == "l1-cache-line-size")
        {
            cpu_info.l1_cache_line_size = static_cast<UInt8>(std::stoi(pair.value()));
        }
        else if (key == "l2-cache-size")
        {
            cpu_info.l2_cache_size = static_cast<UInt32>(std::stoull(pair.value()));
        }
        else if (key == "l2-cache-line-size")
        {
            cpu_info.l2_cache_line_size = static_cast<UInt8>(std::stoi(pair.value()));
        }
        else if (key == "l3-cache-size")
        {
            cpu_info.l3_cache_size = static_cast<UInt32>(std::stoull(pair.value()));
        }
        else if (key == "l3-cache-line-size")
        {
            cpu_info.l3_cache_line_size = static_cast<UInt8>(std::stoi(pair.value()));
        }
        else if (key == "cpu-arch")
        {
            cpu_info.arch = pair.value();
        }
    }
}

void ServerInfo::parseDiskInfo(const diagnosticspb::ServerInfoItem & disk_info_item)
{
    Disk disk;
    disk.name = disk_info_item.name();
    for (const auto & pair : disk_info_item.pairs())
    {
        const auto & key = pair.key();
        if (key == "type")
        {
            if (pair.value() == "HDD")
            {
                disk.disk_type = Disk::DiskType::HDD;
            }
            else if (pair.value() == "SSD")
            {
                disk.disk_type = Disk::DiskType::SSD;
            }
            else
            {
                disk.disk_type = Disk::DiskType::UNKNOWN;
            }
        }
        else if (key == "total")
        {
            disk.total_space = static_cast<UInt64>(std::stoull(pair.value()));
        }
        else if (key == "free")
        {
            disk.free_space = static_cast<UInt64>(std::stoull(pair.value()));
        }
        else if (key == "path")
        {
            disk.mount_point = pair.value();
        }
        else if (key == "fstype")
        {
            disk.fs_type = pair.value();
        }
    }
    disk_infos.push_back(disk);
}

void ServerInfo::parseMemoryInfo(const diagnosticspb::ServerInfoItem & memory_info_item)
{
    for (const auto & pair : memory_info_item.pairs())
    {
        if (pair.key() == "capacity")
        {
            memory_info.capacity = std::stoull(pair.value());
        }
    }
}

void ServerInfo::parseSysInfo(const diagnosticspb::ServerInfoResponse & sys_info_response)
{
    for (const auto & item : sys_info_response.items())
    {
        const auto & tp = item.tp();
        if (tp == "cpu")
        {
            parseCPUInfo(item);
        }
        else if (tp == "disk")
        {
            parseDiskInfo(item);
        }
        else if (tp == "memory")
        {
            parseMemoryInfo(item);
        }
    }
}

String ServerInfo::debugString() const
{
    FmtBuffer fmt_buf;
    // append cpu info
    fmt_buf.fmtAppend(
        "CPU: \n"
        "     logical cores: {}\n"
        "     physical cores: {}\n"
        "     frequency: {}\n"
        "     l1 cache size: {}\n"
        "     l1 cache line size: {}\n"
        "     l2 cache size: {}\n"
        "     l2 cache line size: {}\n"
        "     l3 cache size: {}\n"
        "     l3 cache line size: {}\n"
        "     arch: {}\n",
        cpu_info.logical_cores,
        cpu_info.physical_cores,
        cpu_info.frequency,
        cpu_info.l1_cache_size,
        cpu_info.l1_cache_line_size,
        cpu_info.l2_cache_size,
        cpu_info.l2_cache_line_size,
        cpu_info.l3_cache_size,
        cpu_info.l3_cache_line_size,
        cpu_info.arch);
    // append disk info
    {
        const static String disk_type_str[] = {"UNKNOWN", "HDD", "SSD"};
        for (const auto & disk_info : disk_infos)
        {
            fmt_buf.fmtAppend(
                "Disk: \n"
                "     name: {}\n"
                "     type: {}\n"
                "     total space: {}\n"
                "     free space: {}\n"
                "     mount point: {}\n"
                "     fstype: {}\n",
                disk_info.name,
                disk_type_str[static_cast<UInt8>(disk_info.disk_type)],
                disk_info.total_space,
                disk_info.free_space,
                disk_info.mount_point,
                disk_info.fs_type);
        }
    }
    // append memory info
    fmt_buf.fmtAppend(
        "Memory: \n"
        "     capacity: {}\n",
        memory_info.capacity);

    return fmt_buf.toString();
}

} // namespace DB