#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

#include <Flash/Coprocessor/TiDBChunk.h>

namespace DB
{

    class BloomFilter
    {
    public:
        BloomFilter() = default;
        void PushU64(unsigned long long);
        void FinishBuild();
        bool ProbeU64(unsigned long long);

    private:
        std::vector<unsigned long long > BitSet;
        int length;
        int unitSize;
    };

    class FNVhash
    {
    private:
        unsigned long long const offset64        = 14695981039346656037ull;
        unsigned long long const prime64         = 1099511628211;

    public:
        FNVhash() = default;
        void resetHash(unsigned long);
        void myhash(unsigned int,const UInt8 *, unsigned int l);
        UInt64 sum64(unsigned int);

    private:
        std::vector<unsigned long long> hashVal;
    };

} // namespace DB
