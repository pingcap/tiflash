#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>

#include "mpp.pb.h"

namespace tzg
{
struct SnappyStatistic
{
    mutable std::atomic_uint64_t compressed_size{};
    mutable std::atomic_uint64_t uncompressed_size{};
    mutable std::atomic_uint64_t package{};
    mutable std::atomic_int64_t chunck_stream_cnt{}, max_chunck_stream_cnt{};
    mutable mpp::CompressMethod method{};
    mutable std::atomic<std::chrono::steady_clock::duration> durations{}, has_write_dur{};
    mutable std::atomic_uint64_t encode_bytes{}, has_write_rows{};

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

    mpp::CompressMethod getMethod() const
    {
        return method;
    }
    void setMethod(mpp::CompressMethod m)
    {
        method = m;
    }
    int64_t getChunckStreamCnt() const
    {
        return chunck_stream_cnt;
    }
    int64_t getMaxChunckStreamCnt() const
    {
        return max_chunck_stream_cnt;
    }
    void addChunckStreamCnt(int x = 1) const
    {
        chunck_stream_cnt += x;
        max_chunck_stream_cnt = std::max(max_chunck_stream_cnt.load(), chunck_stream_cnt.load());
    }

    void update(uint64_t compressed_size_, uint64_t uncompressed_size_)
    {
        compressed_size += compressed_size_;
        uncompressed_size += uncompressed_size_;
        ++package;
    }

    void load(uint64_t & compressed_size_, uint64_t & uncompressed_size_, uint64_t & package_, mpp::CompressMethod & m) const
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

    void addEncodeInfo(std::chrono::steady_clock::duration d, uint64_t ec)
    {
        std::chrono::steady_clock::duration x = durations.load();
        std::chrono::steady_clock::duration y = x + d;
        for (; !durations.compare_exchange_strong(x, y);)
        {
            y = x + d;
        }
        encode_bytes += ec;
    }
    void addHashPartitionWriter(std::chrono::steady_clock::duration d, uint64_t rows)
    {
        std::chrono::steady_clock::duration x = has_write_dur.load();
        std::chrono::steady_clock::duration y = x + d;
        for (; !has_write_dur.compare_exchange_strong(x, y);)
        {
            y = x + d;
        }
        has_write_rows += rows;
    }
    void getEncodeInfo(std::chrono::steady_clock::duration & d, uint64_t & ec, std::chrono::steady_clock::duration & hash_dur, uint64_t & hash_rows)
    {
        hash_dur = has_write_dur, hash_rows = has_write_rows;
        d = durations;
        ec = encode_bytes;
    }
    void clearEncodeInfo()
    {
        durations.store({});
        has_write_dur.store({});
        encode_bytes = 0;
        has_write_rows = 0;
    }
};
} // namespace tzg