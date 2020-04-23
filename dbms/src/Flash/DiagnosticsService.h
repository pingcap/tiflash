#pragma once

#include <common/logger_useful.h>
#include <boost/noncopyable.hpp>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{


class DiagnosticsService final : public ::diagnosticspb::Diagnostics::Service,
                                 public std::enable_shared_from_this<DiagnosticsService>,
                                 private boost::noncopyable
{
public:
    DiagnosticsService() : log(&Logger::get("DiagnosticsService")) {}
    ~DiagnosticsService() override {}

public:
    ::grpc::Status search_log(::grpc::ServerContext * context, const ::diagnosticspb::SearchLogRequest * request,
        ::grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * writer) override;

    ::grpc::Status server_info(::grpc::ServerContext * context, const ::diagnosticspb::ServerInfoRequest * request,
        ::diagnosticspb::ServerInfoResponse * response) override;

public:
    struct AvgLoad
    {
        double one;
        double five;
        double fifteen;
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
    void cpuLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void memLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void nicLoadInfo(const NICInfo & prev_nic, std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void ioLoadInfo(const IOInfo & prev_io, std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void loadInfo(const NICInfo & prev_nic, const IOInfo & prev_io, std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void cpuHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void memHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void diskHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void nicHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void hardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void systemInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);
    void processInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items);

    Poco::Logger * log;
};

} // namespace DB
