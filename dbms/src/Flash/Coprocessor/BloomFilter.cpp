//
// Created by Shenghui Wu on 2020/3/4.
//

#include "BloomFilter.h"

namespace DB {

    void BloomFilter::PushU64(unsigned long long value) {
        BitSet.push_back(value);
    }

    void BloomFilter::FinishBuild() {
        length = BitSet.size() * 64;
        unitSize = 64;
    }

    bool BloomFilter::ProbeU64(unsigned long long key) {
        auto hash = key % length;
        auto idx = hash / unitSize;
        auto shift = hash % unitSize;
        return ((BitSet[idx]>> shift)&1) != 0;
    }

    void BloomFilter::resetHash() {
        hashVal = offset64;
    }

    void BloomFilter::myhash(const UInt8 * data, unsigned int l) {
        for (unsigned i = 0; i < l; i++) {
            hashVal *= prime64;
            hashVal ^= UInt64(data[i]);
        }
    }

    UInt64 BloomFilter::sum64() {
        return hashVal;
    }

}