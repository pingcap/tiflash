#include <Common/joinStr.h>
#include <DataStreams/Timeline.h>

namespace DB
{
Timeline::Timeline()
{
    for (auto & arr : count)
    {
        std::fill(std::begin(arr), std::end(arr), 0.0);
    }
}

void Timeline::track(CounterType type, Int64 begin_ts, Int64 end_ts, Float64 value)
{
    if (type == NONE)
        return;

    double duration = end_ts - begin_ts;
    for (Int64 ts = begin_ts, step = 0; ts < end_ts; ts += step)
    {
        step = std::min(end_ts - ts, time_slice - ts % time_slice);
        size_t i = (ts / time_slice) % num_buckets;
        double ratio = step / duration;
        count[type][i] += value * ratio;
    }
}

Timeline::Timer::Timer(Timeline & parent_, CounterType type_, bool running_)
    : parent(parent_)
    , type(type_)
    , running(running_)
    , last_tp(Clock::now())
{}

Timeline::Timer::~Timer()
{
    pause();
}

void Timeline::Timer::pause()
{
    auto tp = Clock::now();
    assert(isRunning());
    running = false;
    parent.track(type, last_tp, tp, parent.toNanoseconds(tp - last_tp));
    last_tp = tp;
}

void Timeline::Timer::resume()
{
    assert(!isRunning());
    running = true;
    last_tp = Clock::now();
}

void Timeline::Timer::switchTo(CounterType type_)
{
    pause();
    type = type_;
    resume();
}

Timeline::Timer Timeline::newTimer(CounterType type, bool running)
{
    return Timer(*this, type, running);
}

Float64 Timeline::sum(CounterType type) const
{
    if (type == NONE)
        return 0;

    Float64 result = 0;
    for (const auto & value : count[type])
    {
        result += value;
    }

    return result;
}

void Timeline::dump(FmtBuffer & buf) const
{
    buf.append("{");

    constexpr const char * names[] = {"pull", "self", "push"};
    for (size_t i = 0; i < num_counters; ++i)
    {
        buf.fmtAppend("\"{}\":[", names[i]);
        joinStr(
            std::begin(count[i]),
            std::end(count[i]),
            buf,
            [](Float64 value, FmtBuffer & buf) {
                buf.fmtAppend("{:.1f}", value * 100 / time_slice);
            },
            ",");
        buf.append(i + 1 < num_counters ? "]," : "]");
    }

    buf.append("}");
}
} // namespace DB
