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

    void FNVhash::resetHash(unsigned long num) {
        if (num <= hashVal.size()) {
            for (unsigned long i = 0;i < num;i++) {
                hashVal[i] = offset64;
            }
        } else {
            for (unsigned long i = 0;i < hashVal.size();i++) {
                hashVal[i] = offset64;
            }
            for (unsigned long i = hashVal.size();i < num;i++) {
                hashVal.push_back(offset64);
            }
        }
    }

    void FNVhash::myhash(unsigned int pos, const UInt8 * data, unsigned int l) {
        for (unsigned i = 0; i < l; i++) {
            hashVal[pos] *= prime64;
            hashVal[pos] ^= UInt64(data[i]);
        }
    }

    UInt64 FNVhash::sum64(unsigned int pos) {
        return hashVal[pos];
    }

}