#include <Common/Exception.h>
#include <Flash/DiagnosticsService.h>
#include <Flash/LogSearch.h>
#include <Poco/Path.h>
#include <Storages/PathPool.h>
#include <re2/re2.h>

#include <memory>

#ifdef __linux__

#include <sys/statvfs.h>

#endif

namespace DB
{

using diagnosticspb::LogLevel;
using diagnosticspb::SearchLogResponse;
using diagnosticspb::ServerInfoItem;
using diagnosticspb::ServerInfoPair;
using diagnosticspb::ServerInfoResponse;
using diagnosticspb::ServerInfoType;

namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}

using MemoryInfo = std::map<std::string, uint64_t>;

static constexpr uint KB = 1024;
// static constexpr uint MB = 1024 * 1024;

#ifdef __linux__
static DiagnosticsService::AvgLoad getAvgLoadLinux()
{
    {
        Poco::File avg_load_file("/proc/loadavg");
        if (!avg_load_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/loadavg doesn't exist");
            return DiagnosticsService::AvgLoad{};
        }
    }

    std::ifstream file("/proc/loadavg");
    std::string str;
    std::getline(file, str);
    std::vector<std::string> values;
    boost::split(values, str, boost::is_any_of(" "));
    if (values.size() < 3)
    {
        LOG_WARNING(&Logger::get("DiagnosticsService"), "Cannot parse /proc/loadavg");
        return DiagnosticsService::AvgLoad{};
    }
    return DiagnosticsService::AvgLoad{std::stod(values[0]), std::stod(values[1]), std::stod(values[2])};
}
#endif

static DiagnosticsService::AvgLoad getAvgLoad()
{
#ifdef __linux__
    return getAvgLoadLinux();
#endif
    return {};
}

#ifdef __linux__
static MemoryInfo getMemoryInfoLinux()
{
    MemoryInfo memory_info;
    {
        Poco::File meminfo_file("/proc/meminfo");
        if (!meminfo_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/meminfo doesn't exist");
            return memory_info;
        }
    }
    std::ifstream file("/proc/meminfo");
    std::string line;
    while (std::getline(file, line))
    {
        std::string name;
        std::string kb_str;
        if (RE2::FullMatch(line, R"(([\w\(\)]+):\s+([0-9]+).*)", &name, &kb_str))
        {
            uint64_t kb = std::stoul(kb_str);
            memory_info.emplace(name, kb);
        }
    }
    return memory_info;
}
#endif

static MemoryInfo getMemoryInfo()
{
#ifdef __linux__
    return getMemoryInfoLinux();
#endif
    return {};
}

#ifdef __linux__
static DiagnosticsService::NICInfo getNICInfoLinux()
{
    DiagnosticsService::NICInfo nic_info;
    Poco::File net_dir("/sys/class/net");
    if (!net_dir.exists())
    {
        LOG_WARNING(&Logger::get("DiagnosticsService"), "/sys/class/net doesn't exist");
        return nic_info;
    }

    std::vector<Poco::File> devices;
    net_dir.list(devices);
    for (auto & device : devices)
    {
        std::vector<Poco::File> device_infos;
        device.list(device_infos);
        auto statistics = std::find_if(device_infos.begin(), device_infos.end(), [](const Poco::File & file) {
            Poco::Path path(file.path());
            return path.getFileName() == "statistics";
        });
        std::vector<Poco::File> stat_files;
        statistics->list(stat_files);
        DiagnosticsService::NICLoad load_info{};
        for (auto & stat : stat_files)
        {
            Poco::Path path(stat.path());
            std::ifstream file(path.toString());
            std::string value;
            std::getline(file, value);
            if (path.getFileName() == "rx_bytes")
                load_info.rx_bytes = std::stoul(value);
            else if (path.getFileName() == "tx_bytes")
                load_info.tx_bytes = std::stoul(value);
            else if (path.getFileName() == "rx_packets")
                load_info.rx_packets = std::stoul(value);
            else if (path.getFileName() == "tx_packets")
                load_info.tx_packets = std::stoul(value);
            else if (path.getFileName() == "rx_errors")
                load_info.rx_errors = std::stoul(value);
            else if (path.getFileName() == "tx_errors")
                load_info.tx_errors = std::stoul(value);
            else if (path.getFileName() == "rx_compressed")
                load_info.rx_compressed = std::stoul(value);
            else if (path.getFileName() == "tx_compressed")
                load_info.tx_compressed = std::stoul(value);
        }

        Poco::Path device_path(device.path());
        nic_info.emplace(device_path.getFileName(), load_info);
    }
    return nic_info;
}
#endif

static DiagnosticsService::NICInfo getNICInfo()
{
#ifdef __linux__
    return getNICInfoLinux();
#endif
    return {};
}

#ifdef __linux__
static DiagnosticsService::IOInfo getIOInfoLinux()
{
    DiagnosticsService::IOInfo io_info;
    Poco::File io_dir("/sys/block");
    if (!io_dir.exists())
    {
        LOG_WARNING(&Logger::get("DiagnosticsService"), "/sys/block doesn't exist");
        return io_info;
    }

    std::vector<Poco::File> devices;
    io_dir.list(devices);
    for (auto & device : devices)
    {
        Poco::Path stat_file_path(device.path());
        stat_file_path.append("stat");
        Poco::File stat_file(stat_file_path);
        std::ifstream file(stat_file.path());
        std::string value;
        std::getline(file, value);
        std::vector<std::string> values;
        boost::split(values, value, boost::is_any_of("\t "), boost::token_compress_on);
        if (values.size() < 11)
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "Cannot parse /sys/block");
            return io_info;
        }
        /// TODO: better to initialize with field names
        DiagnosticsService::IOLoad load{std::stod(values[0]), std::stod(values[1]), std::stod(values[2]), std::stod(values[3]),
            std::stod(values[4]), std::stod(values[5]), std::stod(values[6]), std::stod(values[7]), std::stod(values[8]),
            std::stod(values[9]), std::stod(values[10])};

        Poco::Path device_path(device.path());
        io_info.emplace(device_path.getFileName(), load);
    }
    return io_info;
}
#endif

static DiagnosticsService::IOInfo getIOInfo()
{
#ifdef __linux__
    return getIOInfoLinux();
#endif
    return {};
}

#ifdef __linux__
static size_t getPhysicalCoreNumberLinux()
{
    {
        Poco::File info_file("/proc/cpuinfo");
        if (!info_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/cpuinfo doesn't exist");
            return 0;
        }
    }

    std::ifstream file("/proc/cpuinfo");
    std::string line;
    std::unordered_map<uint, size_t> map;
    uint physid = 0;
    size_t cores = 0;
    int chgcount = 0;
    while (std::getline(file, line))
    {
        std::vector<std::string> values;
        boost::split(values, line, boost::is_any_of(":"), boost::token_compress_on);
        if (values.size() >= 2)
        {
            boost::trim_left(values[0]);
            boost::trim_right(values[0]);
            boost::trim_left(values[1]);
            boost::trim_right(values[1]);
            std::string key = values[0];
            std::string value = values[1];
            if (key == "physical id")
            {
                physid = std::stoul(value);
                chgcount += 1;
            }
            else if (key == "cpu cores")
            {
                cores = std::stoul(value);
                chgcount += 1;
            }
            if (chgcount == 2)
            {
                map.insert({physid, cores});
                chgcount = 0;
            }
        }
    }

    size_t count = 0;
    for (auto [key, value] : map)
    {
        (void)key;
        count += value;
    }
    return count;
}
#endif

static size_t getPhysicalCoreNumber()
{
#ifdef __linux__
    return getPhysicalCoreNumberLinux();
#endif
    return 0;
}

#ifdef __linux__
static uint64_t getCPUFrequencyLinux()
{
    {
        Poco::File info_file("/proc/cpuinfo");
        if (!info_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/cpuinfo doesn't exist");
            return 0;
        }
    }

    std::ifstream file("/proc/cpuinfo");
    std::string line;
    while (std::getline(file, line))
    {
        std::vector<std::string> values;
        boost::split(values, line, boost::is_any_of(":\t"), boost::token_compress_on);
        if (values.size() >= 2)
        {
            std::string key = values[0];
            std::string value = values[1];
            if (key == "cpu MHz" || key == "BogoMIPS" || key == "clock" || key == "bogomips per cpu")
            {
                boost::replace_all(value, "MHz", "");
                double freq = std::stod(value);
                return static_cast<uint64_t>(freq);
            }
        }
    }
    return 0;
}
#endif

static uint64_t getCPUFrequency()
{
#ifdef __linux__
    return getCPUFrequencyLinux();
#endif
    return 0;
}

#ifdef __linux__
static void getCacheSizeLinux(const uint & level, size_t & size, size_t & line_size)
{
    Poco::Path cache_dir("/sys/devices/system/cpu/cpu0/cache/index" + std::to_string(level));
    Poco::Path size_path = cache_dir;
    size_path.append("size");
    Poco::Path line_size_path = cache_dir;
    line_size_path.append("coherency_line_size");

    std::ifstream size_file(size_path.toString());
    std::string size_str;
    std::getline(size_file, size_str);
    // Trim the last 'K'
    size_str.resize(size_str.size() - 1);
    size = std::stoul(size_str) * KB;

    std::ifstream line_size_file(line_size_path.toString());
    std::string line_size_str;
    std::getline(line_size_file, line_size_str);
    line_size = std::stoul(line_size_str);
}
#endif

static void getCacheSize([[maybe_unused]] const uint & level, size_t & size, size_t & line_size)
{
#ifdef __linux__
    getCacheSizeLinux(level, size, line_size);
#else
    size = 0;
    line_size = 0;
#endif
    return;
}

#ifdef __linux__
static DiagnosticsService::Disk::DiskType getDiskTypeByNameLinux(const std::string & name)
{
    // Reference: https://github.com/GuillaumeGomez/sysinfo/blob/28c9e1071eb26c02154d584759f3ddd1e1da4ddf/src/linux/disk.rs#L101-L110
    // The format of devices are as follows:
    //  - name_path is symbolic link in the case of /dev/mapper/
    //     and /dev/root, and the target is corresponding device under
    //     /sys/block/
    //  - In the case of /dev/sd, the format is /dev/sd[a-z][1-9],
    //     corresponding to /sys/block/sd[a-z]
    //  - In the case of /dev/nvme, the format is /dev/nvme[0-9]n[0-9]p[0-9],
    //     corresponding to /sys/block/nvme[0-9]n[0-9]
    //  - In the case of /dev/mmcblk, the format is /dev/mmcblk[0-9]p[0-9],
    //     corresponding to /sys/block/mmcblk[0-9]
    std::string real_path = Poco::Path(name).absolute().toString();
    if (boost::starts_with(name, "/dev/mapper/"))
    {
        // Recursively solve, for example /dev/dm-0
        if (real_path != name)
            return getDiskTypeByNameLinux(real_path);
    }
    else if (boost::starts_with(name, "/dev/sd") || boost::starts_with(name, "/dev/vd"))
    {
        // Turn "sda1" into "sda"
        if (boost::starts_with(real_path, "/dev/"))
            real_path = real_path.substr(5);
        boost::trim_right_if(real_path, boost::is_digit());
    }
    else if (boost::starts_with(name, "/dev/nvme"))
    {
        // Turn "nvme0n1p1" into "nvme0n1"
        if (boost::starts_with(real_path, "/dev/"))
            real_path = real_path.substr(5);
        boost::trim_right_if(real_path, boost::is_digit());
        boost::trim_right_if(real_path, boost::is_any_of("p"));
    }
    else if (boost::starts_with(name, "/dev/root"))
    {
        // Recursively solve, for example /dev/mmcblk0p1
        if (real_path != name)
            return getDiskTypeByNameLinux(real_path);
    }
    else if (boost::starts_with(name, "/dev/mmcblk"))
    {
        // Recursively solve, for example /dev/dm-0
        if (boost::starts_with(real_path, "/dev/"))
            real_path = real_path.substr(5);
        boost::trim_right_if(real_path, boost::is_digit());
        boost::trim_right_if(real_path, boost::is_any_of("p"));
    }
    else
    {
        // Trim /dev/ by default
        if (boost::starts_with(real_path, "/dev/"))
            real_path = real_path.substr(5);
    }
    Poco::Path rotational_file_path("/sys/block/" + real_path + "/queue/rotational");
    if (Poco::File(rotational_file_path).exists())
    {
        std::ifstream rotational_file(rotational_file_path.toString());
        std::string rotational_buffer;
        std::getline(rotational_file, rotational_buffer);
        int rotational = std::stoi(rotational_buffer);
        return rotational == 1 ? DiagnosticsService::Disk::DiskType::HDD : DiagnosticsService::Disk::DiskType::SSD;
    }
    return DiagnosticsService::Disk::DiskType::UNKNOWN;
}

static std::vector<DiagnosticsService::Disk> getAllDisksLinux()
{
    std::vector<DiagnosticsService::Disk> disks;
    {
        Poco::File mount_file("/proc/mounts");
        if (!mount_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/mounts doesn't exist");
            return disks;
        }
    }

    // http://man7.org/linux/man-pages/man5/fstab.5.html
    // /proc/mounts fields(from left to right):
    // - fs_spec: Device specification(e.g. /dev/sda)
    // - fs_file: Mount point
    // - fs_vfstype: File system
    // - fs_mntops: Read-only(ro), Read-write(rw) or else
    // - fs_freq
    // - fs_passno
    std::ifstream mount_file("/proc/mounts");
    std::string mount_info;
    while (std::getline(mount_file, mount_info))
    {
        // TODO: use string_view instead
        std::vector<std::string> values;
        DiagnosticsService::Disk disk;
        boost::split(values, mount_info, boost::is_any_of("\t "), boost::token_compress_on);
        if (values.size() < 3)
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "Cannot parse /proc/mounts");
            continue;
        }
        static std::vector<std::string> fs_filter{
            "sysfs", // pseudo file system for kernel objects
            "proc",  // another pseudo file system
            "tmpfs", "devtmpfs", "cgroup", "cgroup2",
            "pstore",     // https://www.kernel.org/doc/Documentation/ABI/testing/pstore
            "squashfs",   // squashfs is a compressed read-only file system (for snaps)
            "rpc_pipefs", // The pipefs pseudo file system service
            "iso9660"     // optical media
        };
        if (std::find(fs_filter.begin(), fs_filter.end(), values[2]) != fs_filter.end() || boost::starts_with(values[1], "/sys")
            || boost::starts_with(values[1], "/proc")
            || (boost::starts_with(values[1], "/run") && !boost::starts_with(values[1], "/run/media"))
            || boost::starts_with(values[1], "sunrpc"))
        {
            // Ignore unsupported fs type
            continue;
        }

        disk.name = values[0];
        disk.mount_point = values[1];
        disk.disk_type = getDiskTypeByNameLinux(disk.name);
        disk.fs_type = values[2];

        struct statvfs stat;
        statvfs(disk.mount_point.data(), &stat);
        size_t total = static_cast<size_t>(stat.f_blocks) * static_cast<size_t>(stat.f_bsize);
        size_t available = static_cast<size_t>(stat.f_bavail) * static_cast<size_t>(stat.f_bsize);

        disk.total_space = total;
        disk.available_space = available;

        disks.emplace_back(std::move(disk));
    }
    return disks;
}
#endif

static std::vector<DiagnosticsService::Disk> getAllDisks()
{
#ifdef __linux__
    return getAllDisksLinux();
#endif
    return {};
}

void DiagnosticsService::cpuLoadInfo(
    std::optional<DiagnosticsService::LinuxCpuTime> prev_cpu_time, std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    {
        Poco::File stat_file("/proc/stat");
        if (!stat_file.exists())
        {
            // Ignore error since some os doesn't have /proc/stat
            LOG_WARNING(log, "/proc/stat doesn't exist");
            return;
        }
    }

    // Get CPU load
    {
        auto avg_load = getAvgLoad();
        std::vector<std::pair<std::string, double>> names{{"load1", avg_load.one}, {"load5", avg_load.five}, {"load15", avg_load.fifteen}};
        std::vector<ServerInfoPair> pairs;
        for (auto & name : names)
        {
            ServerInfoPair pair;
            pair.set_key(name.first);
            pair.set_value(std::to_string(name.second));
            pairs.emplace_back(std::move(pair));
        }
        ServerInfoItem item;
        for (auto & pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        item.set_tp("cpu");
        item.set_name("cpu");
        server_info_items.emplace_back(std::move(item));
    }

    // Get CPU stats
    {
        if (!prev_cpu_time)
        {
            return;
        }

        auto cpu_time = LinuxCpuTime::current();
        if (!cpu_time || cpu_time->total() == 0)
        {
            return;
        }

        LinuxCpuTime delta{};
        delta.user = cpu_time->user - prev_cpu_time->user;
        delta.nice = cpu_time->nice - prev_cpu_time->nice;
        delta.system = cpu_time->system - prev_cpu_time->system;
        delta.idle = cpu_time->idle - prev_cpu_time->idle;
        delta.iowait = cpu_time->iowait - prev_cpu_time->iowait;
        delta.irq = cpu_time->irq - prev_cpu_time->irq;
        delta.softirq = cpu_time->softirq - prev_cpu_time->softirq;
        delta.steal = cpu_time->steal - prev_cpu_time->steal;
        delta.guest = cpu_time->guest - prev_cpu_time->guest;
        delta.guest_nice = cpu_time->guest_nice - prev_cpu_time->guest_nice;

        auto delta_total = static_cast<double>(delta.total());

        std::vector<std::pair<std::string, double>> data{{"user", static_cast<double>(delta.user) / delta_total},
            {"nice", static_cast<double>(delta.nice) / delta_total}, {"system", static_cast<double>(delta.system) / delta_total},
            {"idle", static_cast<double>(delta.idle) / delta_total}, {"iowait", static_cast<double>(delta.iowait) / delta_total},
            {"irq", static_cast<double>(delta.irq) / delta_total}, {"softirq", static_cast<double>(delta.softirq) / delta_total},
            {"steal", static_cast<double>(delta.steal) / delta_total}, {"guest", static_cast<double>(delta.guest) / delta_total},
            {"guest_nice", static_cast<double>(delta.guest_nice) / delta_total}};

        std::vector<ServerInfoPair> pairs;
        for (auto & p : data)
        {
            ServerInfoPair pair;
            pair.set_key(p.first);
            char buff[20];
            std::sprintf(buff, "%.2lf", p.second);
            pair.set_value(buff);
            pairs.emplace_back(std::move(pair));
        }
        ServerInfoItem item;
        item.set_tp("cpu");
        item.set_name("usage");
        for (auto & pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        server_info_items.emplace_back(std::move(item));
    }
}

/// TODO: wrap MemoryInfo with struct
void DiagnosticsService::memLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    auto meminfo = getMemoryInfo();

    /// TODO: we should check both cgroup quota and MemTotal, then pick the less one

    uint64_t buffers = meminfo.at("Buffers") * KB;
    uint64_t cached = meminfo.at("Cached") * KB;

    uint64_t total_memory = meminfo.at("MemTotal") * KB;
    uint64_t free_memory = meminfo.at("MemFree") * KB;
    uint64_t used_memory = total_memory - free_memory - buffers - cached;
    uint64_t total_swap = meminfo.at("SwapTotal") * KB;
    uint64_t free_swap = meminfo.at("SwapFree") * KB;
    uint64_t used_swap = total_swap - free_swap;

    char buffer[10];
    double used_memory_percent = static_cast<double>(used_memory) / static_cast<double>(total_memory);
    std::sprintf(buffer, "%.2lf", used_memory_percent);
    std::string used_memory_percent_str(buffer);
    double free_memory_percent = static_cast<double>(free_memory) / static_cast<double>(total_memory);
    std::sprintf(buffer, "%.2lf", free_memory_percent);
    std::string free_memory_percent_str(buffer);
    double used_swap_percent = static_cast<double>(used_swap) / static_cast<double>(total_swap);
    std::sprintf(buffer, "%.2lf", used_swap_percent);
    std::string used_swap_percent_str(buffer);
    double free_swap_percent = static_cast<double>(free_swap) / static_cast<double>(total_swap);
    std::sprintf(buffer, "%.2lf", free_swap_percent);
    std::string free_swap_percent_str(buffer);

    std::vector<std::pair<std::string, std::string>> memory_infos{//
        {"total", std::to_string(total_memory)},                  //
        {"free", std::to_string(free_memory)},                    //
        {"used", std::to_string(used_memory)},                    //
        {"free-percent", free_memory_percent_str},                //
        {"used-percent", used_memory_percent_str}};

    std::vector<std::pair<std::string, std::string>> swap_infos{//
        {"total", std::to_string(total_swap)},                  //
        {"free", std::to_string(free_swap)},                    //
        {"used", std::to_string(used_swap)},                    //
        {"free-percent", free_swap_percent_str},                //
        {"used-percent", used_swap_percent_str}};

    // Add pairs for memory_infos
    {
        std::vector<ServerInfoPair> pairs;
        for (auto & info : memory_infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            pair.set_value(info.second);
            pairs.emplace_back(std::move(pair));
        }

        ServerInfoItem item;
        item.set_name("virtual");
        item.set_tp("memory");
        for (auto & pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        server_info_items.emplace_back(std::move(item));
    }

    // Add pairs for swap_infos
    {
        std::vector<ServerInfoPair> pairs;
        for (auto & info : swap_infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            pair.set_value(info.second);
            pairs.emplace_back(std::move(pair));
        }

        ServerInfoItem item;
        item.set_name("swap");
        item.set_tp("memory");
        for (auto & pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::nicLoadInfo(const NICInfo & prev_nic, std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    auto rate = [](size_t prev, size_t cur) { return static_cast<double>(cur - prev) / 0.5; };
    NICInfo nic_info = getNICInfo();
    for (auto [name, cur] : nic_info)
    {
        auto prev = prev_nic.find(name);
        if (prev == prev_nic.end())
            continue;
        std::vector<std::pair<std::string, double>> infos{
            {"rx-bytes/s", rate(cur.rx_bytes, prev->second.rx_bytes)},
            {"tx-bytes/s", rate(cur.tx_bytes, prev->second.tx_bytes)},
            {"rx-packets/s", rate(cur.rx_packets, prev->second.rx_packets)},
            {"tx-packets/s", rate(cur.tx_packets, prev->second.tx_packets)},
            {"rx-errors/s", rate(cur.rx_errors, prev->second.rx_errors)},
            {"tx-errors/s", rate(cur.tx_errors, prev->second.tx_errors)},
            {"rx-comp/s", rate(cur.rx_compressed, prev->second.rx_compressed)},
            {"tx-comp/s", rate(cur.tx_compressed, prev->second.tx_compressed)},
        };
        std::vector<ServerInfoPair> pairs;
        for (auto & info : infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            char buffer[10];
            std::sprintf(buffer, "%.2lf", info.second);
            pair.set_value(std::string(buffer));
            pairs.emplace_back(std::move(pair));
        }
        ServerInfoItem item;
        for (auto & pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        item.set_tp("net");
        item.set_name(name);
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::ioLoadInfo(
    const DiagnosticsService::IOInfo & prev_io, std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    auto rate = [](size_t prev, size_t cur) { return static_cast<double>(cur - prev) / 0.5; };
    IOInfo io_info = getIOInfo();
    for (auto [name, cur] : io_info)
    {
        auto prev = prev_io.find(name);
        if (prev == prev_io.end())
            continue;
        std::vector<std::pair<std::string, double>> infos{
            {"read_io/s", rate(cur.read_io, prev->second.read_io)},
            {"read_merges/s", rate(cur.read_merges, prev->second.read_merges)},
            {"read_sectors/s", rate(cur.read_sectors, prev->second.read_sectors) * 512.0},
            {"read_ticks/s", rate(cur.read_ticks, prev->second.read_ticks)},
            {"write_io/s", rate(cur.write_io, prev->second.write_ticks)},
            {"write_merges/s", rate(cur.write_merges, prev->second.write_merges)},
            {"write_sectors/s", rate(cur.write_sectors, prev->second.write_sectors) * 512.0},
            {"write_ticks/s", rate(cur.write_ticks, prev->second.write_ticks)},
            {"in_flght/s", rate(cur.in_flight, prev->second.in_flight)},
            {"io_ticks/s", rate(cur.io_ticks, prev->second.io_ticks)},
            {"time_in_queue/s", rate(cur.time_in_queue, prev->second.time_in_queue)},
        };
        std::vector<ServerInfoPair> pairs;
        for (auto & info : infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            char buffer[10];
            std::sprintf(buffer, "%.2lf", info.second);
            pair.set_value(std::string(buffer));
            pairs.emplace_back(std::move(pair));
        }
        ServerInfoItem item;
        for (auto & pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        item.set_tp("io");
        item.set_name(name);
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::loadInfo(std::optional<DiagnosticsService::LinuxCpuTime> prev_cpu_time, const NICInfo & prev_nic,
    const IOInfo & prev_io, std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    cpuLoadInfo(prev_cpu_time, server_info_items);
    memLoadInfo(server_info_items);
    (void)prev_nic;
    (void)prev_io;
    // nicLoadInfo(prev_nic, server_info_items);
    // ioLoadInfo(prev_io, server_info_items);
}

void DiagnosticsService::cpuHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    std::vector<std::pair<std::string, std::string>> infos{//
        {"cpu-logical-cores",
            std::to_string(std::thread::hardware_concurrency())}, /// TODO: we should check both machine's core number and cgroup quota
        {"cpu-physical-cores", std::to_string(getPhysicalCoreNumber())}, //
        {"cpu-frequency", std::to_string(getCPUFrequency()) + "MHz"}};

    // L1 to L3 cache
    for (uint8_t level = 1; level <= 3; level++)
    {
        size_t cache_size;
        size_t cache_line_size;
        getCacheSize(level, cache_size, cache_line_size);
        std::string level_str = "l" + std::to_string(level);
        infos.emplace_back(level_str + "-cache-size", std::to_string(cache_size));
        infos.emplace_back(level_str + "-cache-line-size", std::to_string(cache_line_size));
    }

    ServerInfoItem item;
    for (auto & info : infos)
    {
        auto pair = item.add_pairs();
        pair->set_key(info.first);
        pair->set_value(info.second);
    }
    item.set_name("cpu");
    item.set_tp("cpu");
    server_info_items.emplace_back(std::move(item));
}

void DiagnosticsService::memHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    auto mem_info = getMemoryInfo();
    size_t total_mem = mem_info.at("MemTotal");

    ServerInfoItem item;
    auto pair = item.add_pairs();
    pair->set_key("capacity");
    pair->set_value(std::to_string(total_mem * KB));
    item.set_name("memory");
    item.set_tp("memory");

    server_info_items.emplace_back(std::move(item));
}

void DiagnosticsService::diskHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    std::vector<Disk> all_disks = getAllDisks();

    auto & path_pool = server.context().getPathPool();
    // TODO: Maybe add deploy path of both TiFlash and Proxy
    std::vector<std::string> data_dirs = path_pool.listPaths();

    std::unordered_map<std::string, Disk> disks_in_use;
    disks_in_use.reserve(all_disks.size());

    for (auto & dir : data_dirs)
    {
        size_t max_prefix_length = 0;
        int mount_disk_index = -1;
        for (size_t i = 0; i < all_disks.size(); i++)
        {
            auto & disk = all_disks[i];
            if (boost::starts_with(dir, disk.mount_point) && disk.mount_point.size() > max_prefix_length)
            {
                max_prefix_length = disk.mount_point.size();
                mount_disk_index = i;
            }
        }
        if (mount_disk_index < 0)
        {
            LOG_WARNING(log, "Cannot find mounted disk of path: " + dir);
            continue;
        }
        auto & disk = all_disks[mount_disk_index];
        disks_in_use.try_emplace(disk.mount_point, disk);
    }

    for (auto & [mount_point, disk] : disks_in_use)
    {
        size_t total = disk.total_space;
        size_t free = disk.available_space;
        size_t used = total - free;

        char buffer[10];
        double free_percent = static_cast<double>(free) / static_cast<double>(total);
        std::sprintf(buffer, "%.2lf", free_percent);
        std::string free_percent_str(buffer);
        double used_percent = static_cast<double>(used) / static_cast<double>(total);
        std::sprintf(buffer, "%.2lf", used_percent);
        std::string used_percent_str(buffer);

        std::vector<std::pair<std::string, std::string>> infos{{"type", disk.disk_type == disk.HDD ? "HDD" : "SSD"},
            {"fstype", disk.fs_type}, {"path", mount_point}, {"total", std::to_string(total)}, {"free", std::to_string(free)},
            {"used", std::to_string(used)}, {"free-percent", free_percent_str}, {"used-percent", used_percent_str}};

        ServerInfoItem item;
        for (auto & info : infos)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(info.first);
            added_pair->set_value(info.second);
        }
        item.set_tp("disk");
        item.set_name(disk.name);
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::nicHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items) { (void)server_info_items; }

void DiagnosticsService::hardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    cpuHardwareInfo(server_info_items);
    memHardwareInfo(server_info_items);
    diskHardwareInfo(server_info_items);
}

void DiagnosticsService::systemInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items) { (void)server_info_items; }

void DiagnosticsService::processInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items) { (void)server_info_items; }

::grpc::Status DiagnosticsService::server_info(
    ::grpc::ServerContext * context, const ::diagnosticspb::ServerInfoRequest * request, ::diagnosticspb::ServerInfoResponse * response)
try
{
    (void)context;

    auto tp = request->tp();
    std::vector<ServerInfoItem> items;

    std::optional<LinuxCpuTime> cpu_info;
    IOInfo io_info;
    NICInfo nic_info;

    if (tp == ServerInfoType::LoadInfo || tp == ServerInfoType::All)
    {
        // io_info = getIOInfo();
        // nic_info = getNICInfo();
        cpu_info = LinuxCpuTime::current();
        // Sleep 100ms to sample
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    switch (tp)
    {
        case ServerInfoType::LoadInfo:
        {
            loadInfo(cpu_info, nic_info, io_info, items);
            break;
        }
        case ServerInfoType::HardwareInfo:
        {
            hardwareInfo(items);
            break;
        }
        case ServerInfoType::SystemInfo:
        {
            systemInfo(items);
            break;
        }
        case ServerInfoType::All:
        {
            loadInfo(cpu_info, nic_info, io_info, items);
            hardwareInfo(items);
            systemInfo(items);
            break;
        }
        default:
            break;
    }

    std::sort(items.begin(), items.end(), [](const ServerInfoItem & lhs, const ServerInfoItem & rhs) -> bool {
        auto l = std::make_pair(lhs.tp(), lhs.name());
        auto r = std::make_pair(rhs.tp(), rhs.name());
        return l < r;
    });

    // auto resp = ServerInfoResponse::default_instance();
    auto & resp = *response;
    for (auto & item : items)
    {
        auto added_item = resp.add_items();
        added_item->set_name(item.name());
        added_item->set_tp(item.tp());
        for (auto & pair : item.pairs())
        {
            auto added_pair = added_item->add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
    }

    return ::grpc::Status::OK;
}
catch (const Exception & e)
{
    LOG_ERROR(log, e.displayText());
    return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
}

::grpc::Status DiagnosticsService::search_log(::grpc::ServerContext * grpc_context, const ::diagnosticspb::SearchLogRequest * request,
    ::grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * stream)
{
    (void)grpc_context;

    /// TODO: add error log
    Poco::File log_file(Poco::Path(server.config().getString("logger.log")));
    if (!log_file.exists())
    {
        LOG_ERROR(log, "Invalid log path: " << log_file.path());
        return ::grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
    }

    int64_t start_time = request->start_time();
    int64_t end_time = request->end_time();
    if (end_time == 0)
    {
        end_time = std::numeric_limits<int64_t>::max();
    }
    std::vector<LogLevel> levels;
    for (auto level : request->levels())
    {
        levels.push_back(static_cast<LogLevel>(level));
    }

    std::vector<std::string> patterns;
    for (auto && pattern : request->patterns())
    {
        patterns.push_back(pattern);
    }

    auto in_ptr = std::make_shared<std::ifstream>(log_file.path());

    LogIterator log_itr(start_time, end_time, levels, patterns, in_ptr);

    static constexpr size_t LOG_BATCH_SIZE = 256;

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling SearchLog: " << request->DebugString());
    for (;;)
    {
        size_t i = 0;
        auto resp = SearchLogResponse::default_instance();
        while (auto log_msg = log_itr.next())
        {
            i++;
            auto added_msg = resp.add_messages();
            *added_msg = *log_msg;

            if (i == LOG_BATCH_SIZE - 1)
                break;
        }

        if (i == 0)
            break;

        if (!stream->Write(resp))
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Write response failed for unknown reason.");
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write response failed for unknown reason.");
        }
    }
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling SearchLog done: " << request->DebugString());

    return ::grpc::Status::OK;
}

} // namespace DB
