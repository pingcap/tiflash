#pragma once

#include <Poco/File.h>
#include <Server/IServer.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/noncopyable.hpp>
#include <fstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{


class DiagnosticsService final : public ::diagnosticspb::Diagnostics::Service,
                                 public std::enable_shared_from_this<DiagnosticsService>,
                                 private boost::noncopyable
{
public:
    DiagnosticsService(IServer & _server) : log(&Logger::get("DiagnosticsService")), server(_server) {}
    ~DiagnosticsService() override {}

public:
    ::grpc::Status search_log(::grpc::ServerContext * grpc_context, const ::diagnosticspb::SearchLogRequest * request,
        ::grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * stream) override;

    ::grpc::Status server_info(::grpc::ServerContext * grpc_context, const ::diagnosticspb::ServerInfoRequest * request,
        ::diagnosticspb::ServerInfoResponse * response) override;

public:
    struct AvgLoad
    {
        double one;
        double five;
        double fifteen;
    };

    struct LinuxCpuTime
    {
        uint64_t user;
        uint64_t nice;
        uint64_t system;
        uint64_t idle;
        uint64_t iowait;
        uint64_t irq;
        uint64_t softirq;
        uint64_t steal;
        uint64_t guest;
        uint64_t guest_nice;

        static std::optional<LinuxCpuTime> current()
        {
            {
                Poco::File stat_file("/proc/stat");
                if (!stat_file.exists())
                {
                    // Ignore error since some os doesn't have /proc/stat
                    LOG_WARNING(&Poco::Logger::get("DiagnosticsService"), "/proc/stat doesn't exist");
                    return {};
                }
            }

            std::ifstream file("/proc/stat");
            std::string line;
            if (std::getline(file, line))
            {
                std::vector<std::string> values;
                boost::split(values, line, boost::is_any_of(" \t"), boost::token_compress_on);
                if (values.size() == 0 || values[0] != "cpu" || values.size() < 11)
                {
                    return {};
                }

                LinuxCpuTime cpu_time;
                cpu_time.user = std::stoul(values[1]);
                cpu_time.nice = std::stoul(values[2]);
                cpu_time.system = std::stoul(values[3]);
                cpu_time.idle = std::stoul(values[4]);
                cpu_time.iowait = std::stoul(values[5]);
                cpu_time.irq = std::stoul(values[6]);
                cpu_time.softirq = std::stoul(values[7]);
                cpu_time.steal = std::stoul(values[8]);
                cpu_time.guest = std::stoul(values[9]);
                cpu_time.guest_nice = std::stoul(values[10]);

                return cpu_time;
            }

            return {};
        }

        uint64_t total()
        {
            // Note: guest(_nice) is not counted, since it is already in user.
            // See https://unix.stackexchange.com/questions/178045/proc-stat-is-guest-counted-into-user-time
            return user + nice + system + idle + iowait + irq + softirq + steal;
        }
    };

    struct NICLoad
    {
        size_t rx_bytes;
        size_t tx_bytes;
        size_t rx_packets;
        size_t tx_packets;
        size_t rx_errors;
        size_t tx_errors;
        size_t rx_compressed;
        size_t tx_compressed;
    };
    using NICInfo = std::unordered_map<std::string, NICLoad>;

    struct IOLoad
    {
        /// number of read I/Os processed
        /// units: requests
        double read_io;
        /// number of read I/Os merged with in-queue I/O
        /// units: requests
        double read_merges;
        /// number of sectors read
        /// units: sectors
        double read_sectors;
        /// total wait time for read requests
        /// units: milliseconds
        double read_ticks;
        /// number of write I/Os processed
        /// units: requests
        double write_io;
        /// number of write I/Os merged with in-queue I/O
        /// units: requests
        double write_merges;
        /// number of sectors written
        /// units: sectors
        double write_sectors;
        /// total wait time for write requests
        /// units: milliseconds
        double write_ticks;
        /// number of I/Os currently in flight
        /// units: requests
        double in_flight;
        /// total time this block device has been active
        /// units: milliseconds
        double io_ticks;
        /// total wait time for all requests
        /// units: milliseconds
        double time_in_queue;
    };
    using IOInfo = std::unordered_map<std::string, IOLoad>;

    struct Disk
    {
        enum DiskType
        {
            HDD,
            SSD,
            UNKNOWN
        };

        DiskType disk_type;
        std::string name;
        size_t total_space;
        size_t available_space;
        std::string mount_point;
        std::string fs_type;
    };

    struct NetworkInterface
    {
        std::string name;
        std::vector<uint8_t> mac;
        uint flag;
        uint index;
        bool is_up;
        bool is_broadcast;
        bool is_multicast;
        bool is_loopback;
        bool is_point_to_point;
    };

public:
    void cpuLoadInfo(std::optional<LinuxCpuTime> prev_cpu_time, std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void memLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void nicLoadInfo(const NICInfo & prev_nic, std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void ioLoadInfo(const IOInfo & prev_io, std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void loadInfo(std::optional<DiagnosticsService::LinuxCpuTime> prev_cpu_time, const NICInfo & prev_nic, const IOInfo & prev_io,
        std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void cpuHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void memHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void diskHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void nicHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void hardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void systemInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void processInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);

private:
    Poco::Logger * log;

    IServer & server;
};

} // namespace DB
