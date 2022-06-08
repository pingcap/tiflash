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
        if (pair.key() == "cpu-logical-cores")
        {
            cpu_info.logical_cores = static_cast<UInt16>(std::stoi(pair.value()));
        }
        else if (pair.key() == "cpu-physical-cores")
        {
            cpu_info.physical_cores = static_cast<UInt16>(std::stoi(pair.value()));
        }
        else if (pair.key() == "cpu-frequency")
        {
            cpu_info.frequency = pair.value();
        }
        else if (pair.key() == "l1-cache-size")
        {
            cpu_info.l1_cache_size = static_cast<UInt32>(std::stoull(pair.value()));
        }
        else if (pair.key() == "l1-cache-line-size")
        {
            cpu_info.l1_cache_line_size = static_cast<UInt8>(std::stoi(pair.value()));
        }
        else if (pair.key() == "l2-cache-size")
        {
            cpu_info.l2_cache_size = static_cast<UInt32>(std::stoull(pair.value()));
        }
        else if (pair.key() == "l2-cache-line-size")
        {
            cpu_info.l2_cache_line_size = static_cast<UInt8>(std::stoi(pair.value()));
        }
        else if (pair.key() == "l3-cache-size")
        {
            cpu_info.l3_cache_size = static_cast<UInt32>(std::stoull(pair.value()));
        }
        else if (pair.key() == "l3-cache-line-size")
        {
            cpu_info.l3_cache_line_size = static_cast<UInt8>(std::stoi(pair.value()));
        }
        else if (pair.key() == "cpu-arch")
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
        if (pair.key() == "type")
        {
            std::unordered_map<String, Disk::DiskType> disk_type_map = {
                {"hdd", Disk::DiskType::HDD},
                {"ssd", Disk::DiskType::SSD},
                {"unknown", Disk::DiskType::UNKNOWN}};
            disk.disk_type = disk_type_map[pair.value()];
        }
        else if (pair.key() == "total")
        {
            disk.total_space = static_cast<UInt64>(std::stoull(pair.value()));
        }
        else if (pair.key() == "free")
        {
            disk.free_space = static_cast<UInt64>(std::stoull(pair.value()));
        }
        else if (pair.key() == "path")
        {
            disk.mount_point = pair.value();
        }
        else if (pair.key() == "fstype")
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

void ServerInfo::parseSysInfo(diagnosticspb::ServerInfoResponse & sys_info_response)
{
    for (const auto & item : sys_info_response.items())
    {
        if (item.tp() == "cpu")
        {
            parseCPUInfo(item);
        }
        else if (item.tp() == "disk")
        {
            parseDiskInfo(item);
        }
        else if (item.tp() == "memory")
        {
            parseMemoryInfo(item);
        }
    }
}

String ServerInfo::debugString() const
{
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("CPU: \n"
                      "     logical_cores: {}\n"
                      "     physical_cores: {}\n"
                      "     frequency: {}\n"
                      "     l1_cache_size: {}\n"
                      "     l1_cache_line_size: {}\n"
                      "     l2_cache_size: {}\n"
                      "     l2_cache_line_size: {}\n"
                      "     l3_cache_size: {}\n"
                      "     l3_cache_line_size: {}\n"
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

    for (const auto & disk_info : disk_infos)
    {
        fmt_buf.fmtAppend("Disk: \n"
                          "     name: {}\n"
                          "     type: {}\n"
                          "     total_space: {}\n"
                          "     free_space: {}\n"
                          "     mount_point: {}\n"
                          "     fs_type: {}\n",
                          disk_info.name,
                          disk_info.disk_type,
                          disk_info.total_space,
                          disk_info.free_space,
                          disk_info.mount_point,
                          disk_info.fs_type);
    }
    fmt_buf.fmtAppend("Memory: \n"
                      "     capacity: {}\n",
                      memory_info.capacity);

    return fmt_buf.toString();
}

} // namespace DB