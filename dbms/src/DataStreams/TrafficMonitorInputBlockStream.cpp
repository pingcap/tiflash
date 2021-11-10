#include <Common/joinStr.h>
#include <DataStreams/TrafficMonitorInputBlockStream.h>

namespace DB
{
void TrafficMonitorInputBlockStream::Trace::track(Int64 value)
{
    size_t i = getIndex();
    bkt[i].value.fetch_add(value);
    bkt[i].count.fetch_add(1);
}

void TrafficMonitorInputBlockStream::Trace::dump(FmtBuffer & buf) const
{
    buf.append("[");

    Int64 i = 0;
    joinStr(
        std::begin(bkt),
        std::end(bkt),
        buf,
        [&i](const Bucket & b, FmtBuffer & buf) {
            double v = b.count > 0 ? static_cast<double>(b.value) / b.count : 0;
            buf.fmtAppend(R"({{"ts":{},"args":{{"size":{}}}}})", i * interval, v);
            i++;
        },
        ",");

    buf.append("]");
}

void TrafficMonitorInputBlockStream::onQueue(size_t size)
{
    trace.track(size);
}

void TrafficMonitorInputBlockStream::dumpProfileInfoImpl(FmtBuffer & buf)
{
    trace.dump(buf);
}

} // namespace DB
