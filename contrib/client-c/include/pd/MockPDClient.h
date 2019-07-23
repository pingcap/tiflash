#pragma once

#include <pd/IClient.h>
#include <limits>

namespace pingcap
{
namespace pd
{

using Clock = std::chrono::system_clock;

class MockPDClient : public IClient
{
public:
    MockPDClient() = default;

    ~MockPDClient() override {}

    uint64_t getGCSafePoint() override { return 1000000000000000; }

    uint64_t getTS() override { return Clock::now().time_since_epoch().count(); }

    std::tuple<metapb::Region, metapb::Peer, std::vector<metapb::Peer>> getRegion(std::string) override { throw "not implemented"; }

    std::tuple<metapb::Region, metapb::Peer, std::vector<metapb::Peer>> getRegionByID(uint64_t) override { throw "not implemented"; }

    metapb::Store getStore(uint64_t) override { throw "not implemented"; }

    bool isMock() override { return true; }
};

} // namespace pd
} // namespace pingcap
