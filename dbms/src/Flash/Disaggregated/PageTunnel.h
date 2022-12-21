#pragma once

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Transaction/Types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/support/sync_stream.h>
#include <kvproto/mpp.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class Context;
namespace DM
{
class DisaggregatedSnapshotManager;
}

using SyncPagePacketWriter = grpc::ServerWriter<mpp::PagesPacket>;

class PageTunnel;
using PageTunnelPtr = std::unique_ptr<PageTunnel>;

class PageTunnel
{
public:
    static PageTunnelPtr build(
        const Context & context,
        const DM::DisaggregatedTaskId & task_id,
        TableID table_id,
        UInt64 segment_id,
        const PageIds & read_page_ids);

    void connect(SyncPagePacketWriter * sync_writer);

    // wait until all the data has been transferred.
    void waitForFinish();

    void close();

public:
    PageTunnel(DM::DisaggregatedTaskId task_id_,
               DM::DisaggregatedSnapshotManager * manager,
               DM::SegmentReadTaskPtr seg_task_,
               PageIds read_page_ids)
        : task_id(std::move(task_id_))
        , snap_manager(manager)
        , seg_task(seg_task_)
        , read_page_ids(std::move(read_page_ids))
        , log(Logger::get())
    {}

    // just for test
    PageTunnel(DM::SegmentReadTaskPtr seg_task_, PageIds read_page_ids)
        : PageTunnel(
            DM::DisaggregatedTaskId::unknown_disaggregated_task_id,
            nullptr,
            std::move(seg_task_),
            std::move(read_page_ids))
    {}

    // just for test
    mpp::PagesPacket readPacket();

private:
    const DM::DisaggregatedTaskId task_id;
    DM::DisaggregatedSnapshotManager * const snap_manager;
    DM::SegmentReadTaskPtr seg_task;
    PageIds read_page_ids;

    LoggerPtr log;
};

} // namespace DB
