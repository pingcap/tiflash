#include <DataStreams/ExchangeSender.h>
namespace DB
{
Block ExchangeSender::readImpl()
{
    auto timer = newTimer(Timeline::PULL);
    Block block = children.back()->read();
    timer.switchTo(Timeline::SELF);

    if (block)
    {
        writer->current_timer = &timer;
        writer->write(block);
        writer->current_timer = nullptr;
    }
    return block;
}
} // namespace DB
