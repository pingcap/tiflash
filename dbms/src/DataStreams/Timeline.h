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

    struct Event
    {
        Int64 begin_ts, end_ts;
        CounterType type;
    };

    UInt64 count[num_counters];
    std::list<Event> events;

    Timeline();

    void track(CounterType type, Int64 begin_ts, Int64 end_ts, Float64 value);

    template <typename TimePoint>
    void track(CounterType type, const TimePoint & begin_tp, const TimePoint & end_tp, Float64 value)
    {
        Int64 begin_ts = toTimestamp(begin_tp);
        Int64 end_ts = toTimestamp(end_tp);
        track(type, begin_ts, end_ts, value);
    }

    class Timer
    {
    public:
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
        using Clock = std::chrono::high_resolution_clock;

        Timeline & parent;
        CounterType type;
        bool running;
        Clock::time_point last_tp;
    };

    Timer newTimer(CounterType type, bool running = true);

    Float64 sum(CounterType type) const;

    void dump(FmtBuffer & buf) const;

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
};

} // namespace DB
