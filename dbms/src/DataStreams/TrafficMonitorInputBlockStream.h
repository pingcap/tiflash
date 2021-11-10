#pragma once

#include <DataStreams/SharedQueryBlockInputStream.h>

#include <chrono>


namespace DB
{
class TrafficMonitorInputBlockStream : public SharedQueryBlockInputStream
{
public:
    TrafficMonitorInputBlockStream(const BlockInputStreamPtr & in_, const LogWithPrefixPtr & log_)
        : SharedQueryBlockInputStream(100, in_, log_)
    {
    }

    String getName() const override { return "TrafficMonitor"; }
    Block getHeader() const override { return children.back()->getHeader(); }

private:
    struct Trace
    {
        static constexpr size_t trace_size = 1024;
        static constexpr size_t interval = 100 * 1000;

        struct Bucket
        {
            std::atomic<Int64> value = 0;
            std::atomic<Int64> count = 0;
        };

        Bucket bkt[trace_size];

        static size_t getIndex()
        {
            auto ts = std::chrono::system_clock::now().time_since_epoch();
            size_t i = std::chrono::duration_cast<std::chrono::microseconds>(ts).count() / interval;
            return i % trace_size;
        }

        void track(Int64 value);
        void dump(FmtBuffer & buf) const;
    };

    Trace trace;

    void onQueue(size_t size) override;
    void dumpProfileInfoImpl(FmtBuffer & buf) override;
};

} // namespace DB
