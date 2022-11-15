#pragma once

#include <atomic>
#include <cstdint>

namespace tzg
{
struct SnappyStatistic
{
    enum class Method : int
    {
        LZ4 = 1,
        LZ4HC = 2, /// The format is the same as for LZ4. The difference is only in compression.
        ZSTD = 3, /// Experimental algorithm: https://github.com/Cyan4973/zstd
        NONE = 4, /// No compression
        SNAPPY = 5, /// Snappy: https://github.com/google/snappy
    };

    mutable std::atomic_uint64_t compressed_size;
    mutable std::atomic_uint64_t uncompressed_size;
    mutable std::atomic_uint64_t package;
    mutable Method method = Method::SNAPPY;

    SnappyStatistic(const SnappyStatistic &) = delete;

    void clear()
    {
        compressed_size = {};
        uncompressed_size = {};
        package = {};
    }

    uint64_t getCompressedSize() const
    {
        return compressed_size;
    }
    uint64_t getUncompressedSize() const
    {
        return uncompressed_size;
    }

    Method getMethod() const
    {
        return method;
    }
    void setMethod(Method m)
    {
        method = m;
    }

    void update(uint64_t compressed_size_, uint64_t uncompressed_size_)
    {
        compressed_size += compressed_size_;
        uncompressed_size += uncompressed_size_;
        ++package;
    }

    void load(uint64_t & compressed_size_, uint64_t & uncompressed_size_, uint64_t & package_, Method & m) const
    {
        compressed_size_ = getCompressedSize();
        uncompressed_size_ = getUncompressedSize();
        package_ = package;
        m = getMethod();
    }

    static SnappyStatistic & globalInstance()
    {
        static SnappyStatistic data{};
        return data;
    }
};
} // namespace tzg