#pragma once

#include <limits>
#include <pd/IClient.h>

namespace pingcap {
namespace pd {

class MockPDClient : public IClient {
public:
    MockPDClient() = default;

    ~MockPDClient() override {}

    uint64_t getGCSafePoint() override
    {
        std::time_t t = std::time(nullptr);
        std::tm & tm = *std::localtime(&t);
        tm.tm_sec -= 2;
        return static_cast<uint64_t>(std::mktime(&tm));
    }

    uint64_t getTS() override
    {
        return static_cast<uint64_t>(std::time(NULL));
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
