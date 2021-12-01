#pragma once

#include <Common/FmtUtils.h>

#include <chrono>


namespace DB
{
class Timeline
{
public:
    enum CounterType : Int64
    {
        NONE = -1,
        PULL,
        SELF,
        PUSH,
        NUM_COUNTER_TYPES
    };

    static String typeToString(CounterType type)
    {
        switch (type)
        {
        case NONE:
            return "none";
        case PULL:
            return "pull";
        case SELF:
            return "self";
        case PUSH:
            return "push";
        default:
            return "<unknown>";
        }
    }

    static constexpr size_t num_counters = static_cast<size_t>(NUM_COUNTER_TYPES);

    Timeline();
    ~Timeline();

    void track(CounterType type, Int64 ts, Int64 value);

    template <typename TimePoint>
    void track(CounterType type, const TimePoint & tp, Int64 value)
    {
        track(type, toTimestamp(tp), value);
    }

    void flushBuffer();

    class Timer
    {
    public:
        using Clock = std::chrono::high_resolution_clock;

        explicit Timer(Timeline & parent_, CounterType type_, bool running_);
        ~Timer();

        void pause(bool do_track = true);
        void resume(bool do_track = true);
        void switchTo(CounterType type_);

        bool isRunning() const
        {
            return running;
        }

    private:
        Timeline & parent;
        CounterType type;
        bool running;
        Clock::time_point last_tp;
    };

    Timer newTimer(CounterType type, bool running = true);

    void dump(FmtBuffer & buf) const;

    Int64 getCounter(CounterType type) const;

private:
    friend class Timer;

    template <typename TimeSpan>
    static Int64 toNanoseconds(const TimeSpan & span)
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(span).count();
    }

    template <typename TimePoint>
    static Int64 toTimestamp(const TimePoint & tp)
    {
        return toNanoseconds(tp.time_since_epoch());
    }

    void checkAndAddEvent(bool is_final = false);

    struct Event
    {
        Int64 ts;
        Int64 count[num_counters];
    };

    Int64 last_ts = 0;
    Int64 batch_threshold = 100'000'000;
    Int64 batched = 0;
    Int64 count[num_counters] = {0}, sum[num_counters] = {0};
    std::list<Event> events;
};

} // namespace DB
