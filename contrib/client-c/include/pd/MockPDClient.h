#pragma once

#include <limits>
#include <pd/IClient.h>

namespace pingcap {
namespace pd {

using Clock = std::chrono::system_clock;
using Seconds = std::chrono::seconds;

class MockPDClient : public IClient {
public:
    MockPDClient() = default;

    ~MockPDClient() override {}

    uint64_t getGCSafePoint() override
    {
        return (Clock::now() - Seconds(2)).time_since_epoch().count();
    }

    uint64_t getTS() override
    {
        return Clock::now().time_since_epoch().count();
    }

    std::tuple<metapb::Region, metapb::Peer, std::vector<metapb::Peer>> getRegion(std::string) override {
        throw "not implemented";
    }

    std::tuple<metapb::Region, metapb::Peer, std::vector<metapb::Peer>> getRegionByID(uint64_t) override {
        throw "not implemented";
    }

    metapb::Store getStore(uint64_t) override {
        throw "not implemented";
    }

    bool isMock() override {
        return true;
    }
};

}
}
