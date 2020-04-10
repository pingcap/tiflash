#include "DiagnosticsService.h"
namespace DB
{

void DiagnosticsService::cpuLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::memLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::nicLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::ioLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::loadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::cpuHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::memHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::diskHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::nicHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::hardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::systemInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}

void DiagnosticsService::processInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_pairs) {}


} // namespace DB