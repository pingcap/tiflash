#pragma once

#include <limits>
#include <Storages/Transaction/PD/IPDClient.h>

namespace DB {

class MockPDClient : public IPDClient {
public:
    MockPDClient() = default;

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

    uint64_t getMaxTS() override
    {
        return std::numeric_limits<uint64_t>::max();
    }
};

}
