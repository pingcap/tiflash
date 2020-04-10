#pragma once

#include <boost/noncopyable.hpp>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

class DiagnosticsService final : public diagnosticspb::Diagnostics::Service,
                                 public std::enable_shared_from_this<DiagnosticsService>,
                                 private boost::noncopyable
{
public:
    grpc::Status search_log(grpc::ServerContext * context, const diagnosticspb::SearchLogRequest * request,
        grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * writer) override;

    grpc::Status server_info(grpc::ServerContext * context, const diagnosticspb::ServerInfoRequest * request,
        diagnosticspb::ServerInfoResponse * response) override;

private:
    void cpuLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void memLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void nicLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void ioLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void loadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void cpuHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void memHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void diskHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void nicHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void hardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void systemInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
    void processInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs);
};

} // namespace DB
