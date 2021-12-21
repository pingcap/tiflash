#include <DataStreams/Timeline.h>

namespace DB
{
Timeline::Timeline()
{
    std::fill(std::begin(count), std::end(count), 0);
    std::fill(std::begin(sum), std::end(sum), 0);
}

Timeline::~Timeline()
{
    checkAndAddEvent(true);
}

void Timeline::checkAndAddEvent(bool is_final)
{
    if (batched > 0 && (is_final || batched >= batch_threshold))
    {
        Event event;
        event.ts = last_ts;
        std::copy(std::begin(count), std::end(count), std::begin(event.count));
        events.push_back(event);

        batched = 0;
        std::fill(std::begin(count), std::end(count), 0);
    }
}

void Timeline::track(CounterType type, Int64 ts, Int64 value)
{
    if (type == NONE)
        return;

    last_ts = ts;
    batched += value;
    count[type] += value;
    sum[type] += value;
    checkAndAddEvent();
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
        parent.track(type, tp, parent.toNanoseconds(tp - last_tp));
    last_tp = tp;
}

void Timeline::Timer::resume(bool do_track)
{
    assert(!isRunning());
    running = true;
    auto tp = Clock::now();
    if (do_track)
        parent.track(Timeline::NONE, tp, parent.toNanoseconds(tp - last_tp));
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

void Timeline::dump(FmtBuffer & buf) const
{
    buf.append("[");
    buf.joinStr(
        events.begin(),
        events.end(),
        [](const Event & event, FmtBuffer & buf) {
            buf.fmtAppend(
                R"({{"ts":{},"pull":{},"self":{},"push":{}}})",
                event.ts,
                event.count[PULL],
                event.count[SELF],
                event.count[PUSH]);
        },
        ",");
    buf.append("]");
}

Int64 Timeline::getCounter(CounterType type) const
{
    if (type == NONE)
        return 0;
    return sum[type];
}

void Timeline::merge(const Timeline & other)
{
    last_ts = std::max(last_ts, other.last_ts);
    for (size_t i = 0; i < num_counters; ++i)
        sum[i] += other.sum[i];

    auto events_iter = events.begin();
    auto other_events_iter = other.events.begin();
    while (events_iter != events.end() && other_events_iter != other.events.end())
    {
        if (events_iter->ts >= other_events_iter->ts)
        {
            for (size_t i = 0; i < num_counters; ++i)
            {
                events_iter->count[i] += other_events_iter->count[i];
            }
            ++other_events_iter;
        }
        else
        {
            ++events_iter;
        }
    }
}

void Timeline::flushBuffer()
{
    checkAndAddEvent(true);
}

Timeline & Timeline::merge(Timeline & left, Timeline & right)
{
    if (left.events.size() >= right.events.size())
    {
        left.merge(right);
        return left;
    }
    else
    {
        right.merge(left);
        return right;
    }
}
} // namespace DB