//
// Created by Shenghui Wu on 2020/3/4.
//

#include "BloomFilter.h"

namespace DB {

    void BloomFilter::PushU64(unsigned long long value) {
        BitSet.push_back(value);
    }

    void BloomFilter::FinishBuild() {
        length = BitSet.size();
        unitSize = 64;
    }

    bool BloomFilter::ProbeU64(unsigned long long key) {
        auto hash = key % length;
        auto idx = hash / unitSize;
        auto shift = hash % unitSize;
        return (BitSet[idx]&(1 << shift)) != 0;
    }
}