#include <Storages/DeltaMerge/Remote/RemotePageInputStream.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM::Remote
{

Page RemotePageInputStream::readCFTinyPage(PageIdU64 page_id)
{
    // auto persist_cf_snap = seg_task->read_snapshot->delta->getPersistedFileSetSnapshot();
    // persist_cf_snap->getStorage()->readForColumnFileTiny(page_id);
    UNUSED(page_id);
    return Page::invalidPage();
}

Block RemotePageInputStream::readMemTableBlock()
{
    UNUSED(expected_block_size);
    return {};
}

} // namespace DB::DM::Remote
