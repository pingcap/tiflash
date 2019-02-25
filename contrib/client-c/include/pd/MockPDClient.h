#pragma once

#include <limits>
#include <pd/IClient.h>

namespace pingcap::pd
{

class MockPDClient : public IClient
{
public:
    MockPDClient(bool enable_gc_ = true) : enable_gc(enable_gc_) {}

    ~MockPDClient() override {}

    uint64_t getGCSafePoint() override
    {
        return enable_gc ? (uint64_t)curr : 0;
        //std::time_t t = std::time(nullptr);
        //std::tm & tm = *std::localtime(&t);
        //tm.tm_sec -= 2;
        //return static_cast<uint64_t>(std::mktime(&tm));
    }

    uint64_t getTS() override
    {
        return curr++;
        //return static_cast<uint64_t>(std::time(NULL));
    }

    std::tuple<metapb::Region, metapb::Peer, metapb::Peer> getRegion(std::string) override
    {
        throw "not implemented";
    }

    std::tuple<metapb::Region, metapb::Peer, metapb::Peer> getRegionByID(uint64_t) override
    {
        throw "not implemented";
    }

    metapb::Store getStore(uint64_t) override
    {
        throw "not implemented";
    }

    // TODO: remove this?
    bool isMock() override
    {
        return true;
    }

private:
    bool enable_gc;
    std::atomic<uint64_t> curr = static_cast<uint64_t>(std::time(NULL));
};

}
