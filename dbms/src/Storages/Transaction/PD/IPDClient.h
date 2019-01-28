#pragma once

namespace DB {

class IPDClient {
public:
    virtual uint64_t getGCSafePoint() = 0;
    virtual uint64_t getTS() = 0;
    virtual uint64_t getMaxTS() = 0;
    virtual ~IPDClient() {}
};

using PDClientPtr = std::shared_ptr<IPDClient>;

}
