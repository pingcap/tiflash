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

} // namespace DB
