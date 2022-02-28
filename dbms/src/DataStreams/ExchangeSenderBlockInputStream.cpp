#include <Common/FailPoint.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>

namespace DB
{
namespace FailPoints
{
extern const char hang_in_execution[];
extern const char exception_during_mpp_non_root_task_run[];
extern const char exception_during_mpp_root_task_run[];
} // namespace FailPoints

Block ExchangeSenderBlockInputStream::readImpl()
{
    FAIL_POINT_PAUSE(FailPoints::hang_in_execution);
    if (writer->dagContext().isRootMPPTask())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_root_task_run);
    }
    else
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_mpp_non_root_task_run);
    }

    Block block = children.back()->read();
    if (block)
        writer->write(block);
    return block;
}
} // namespace DB
