#include <Common/joinStr.h>
#include <DataStreams/Timeline.h>

namespace DB
{
Timeline::Timeline()
{
    std::fill(std::begin(count), std::end(count), 0);
}

void Timeline::track(CounterType type, Int64 begin_ts, Int64 end_ts, Float64 value [[maybe_unused]])
{
    events.push_back({begin_ts, end_ts, type});
    if (type != NONE)
        count[type] += end_ts - begin_ts;
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

void Timeline::Timer::pause(bool do_track)
{
    auto tp = Clock::now();
    assert(isRunning());
    running = false;
    if (do_track)
        parent.track(type, last_tp, tp, parent.toNanoseconds(tp - last_tp));
    last_tp = tp;
}

void Timeline::Timer::resume(bool do_track)
{
    assert(!isRunning());
    running = true;
    auto tp = Clock::now();
    if (do_track)
        parent.track(Timeline::NONE, last_tp, tp, parent.toNanoseconds(tp - last_tp));
    last_tp = tp;
}

void Timeline::Timer::switchTo(CounterType type_)
{
    pause();
    type = type_;
    resume(false);
}

Timeline::Timer Timeline::newTimer(CounterType type, bool running)
{
    return Timer(*this, type, running);
}

Float64 Timeline::sum(CounterType type) const
{
    if (type == NONE)
        return 0;

    return count[type];
}

void Timeline::dump(FmtBuffer & buf) const
{
    buf.append("[");

    joinStr(
        events.begin(),
        events.end(),
        buf,
        [](const Event & event, FmtBuffer & buf) {
            buf.fmtAppend(R"({{"begin_ts_ns":{},"end_ts_ns":{},"type":"{}"}})", event.begin_ts, event.end_ts, typeToString(event.type));
        },
        ",");

    buf.append("]");
}
} // namespace DB
