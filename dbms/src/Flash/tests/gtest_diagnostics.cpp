#include <Flash/DiagnosticsService.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
// using diagnosticspb::ServerInfoItem;
// using diagnosticspb::ServerInfoPair;

// class Diagnostics_Test : public ::testing::Test
// {
// public:
//     Diagnostics_Test() : log(&Logger::get("Diagnostics")), service(DiagnosticsService(Server())) {}

//     DiagnosticsService service;
//     Logger * log;

//     void SetUp() override { return; };
// };


// TEST_F(Diagnostics_Test, DISABLED_ServerInfoTest)
// {
//     std::vector<ServerInfoItem> items;
//     service.cpuLoadInfo(items);
//     std::cout << "CPU Load Information:" << std::endl;
//     for (auto item : items)
//     {
//         std::cout << "name: " << item.name() << ", type: " << item.tp() << std::endl;
//         for (auto pair : item.pairs())
//         {
//             std::cout << pair.key() << " : " << pair.value() << std::endl;
//         }
//         std::cout << std::endl;
//     }

//     items.resize(0);
//     service.memLoadInfo(items);
//     std::cout << "Memory Load Information:" << std::endl;
//     for (auto item : items)
//     {
//         std::cout << "name: " << item.name() << ", type: " << item.tp() << std::endl;
//         for (auto pair : item.pairs())
//         {
//             std::cout << pair.key() << " : " << pair.value() << std::endl;
//         }
//         std::cout << std::endl;
//     }

//     items.resize(0);
//     service.cpuHardwareInfo(items);
//     std::cout << "CPU Hardware Information:" << std::endl;
//     for (auto item : items)
//     {
//         std::cout << "name: " << item.name() << ", type: " << item.tp() << std::endl;
//         for (auto pair : item.pairs())
//         {
//             std::cout << pair.key() << " : " << pair.value() << std::endl;
//         }
//         std::cout << std::endl;
//     }

//     items.resize(0);
//     service.diskHardwareInfo(items);
//     std::cout << "Disk Hardware Information:" << std::endl;
//     for (auto item : items)
//     {
//         std::cout << "name: " << item.name() << ", type: " << item.tp() << std::endl;
//         for (auto pair : item.pairs())
//         {
//             std::cout << pair.key() << " : " << pair.value() << std::endl;
//         }
//         std::cout << std::endl;
//     }

//     items.resize(0);
//     service.memHardwareInfo(items);
//     std::cout << "Memory Hardware Information:" << std::endl;
//     for (auto item : items)
//     {
//         std::cout << "name: " << item.name() << ", type: " << item.tp() << std::endl;
//         for (auto pair : item.pairs())
//         {
//             std::cout << pair.key() << " : " << pair.value() << std::endl;
//         }
//         std::cout << std::endl;
//     }
// }

} // namespace tests
} // namespace DB
