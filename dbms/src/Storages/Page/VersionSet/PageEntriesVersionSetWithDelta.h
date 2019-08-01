#include <utility>

#pragma once

#include <Storages/Page/PageEntries.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/VersionSet/PageEntriesView.h>
#include <Storages/Page/mvcc/VersionSetWithDelta.h>
#include <Storages/Page/mvcc/VersionSet.h>
#include <Storages/Page/VersionSet/PageEntriesEdit.h>

namespace DB
{

class DeltaVersionEditAcceptor;

class PageEntriesVersionSetWithDelta : public ::DB::MVCC::VersionSetWithDelta< //
                                        PageEntriesForDelta,
                                        PageEntriesView,
                                        PageEntriesEdit,
                                        DeltaVersionEditAcceptor>
{
public:
    using BaseType     = ::DB::MVCC::VersionSetWithDelta<PageEntriesForDelta, PageEntriesView, PageEntriesEdit, DeltaVersionEditAcceptor>;
    using EditAcceptor = BaseType::EditAcceptor;
    using VersionType  = BaseType::VersionType;
    using VersionPtr   = BaseType::VersionPtr;

public:
    explicit PageEntriesVersionSetWithDelta(const ::DB::MVCC::VersionSetConfig & config_ = ::DB::MVCC::VersionSetConfig()) : BaseType(config_)
    {
    }

public:
    std::set<PageFileIdAndLevel> gcApply(PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;

    VersionPtr compactDeltas(const VersionPtr & tail) const override;

    VersionPtr compactDeltaAndBase(const VersionPtr & old_base, VersionPtr & delta) const override;

private:
    void collectLiveFilesFromVersionList(VersionPtr tail, std::set<VersionPtr> & visited, std::set<PageFileIdAndLevel> & liveFiles) const;
};

/// Read old entries state from `view_` and apply new edit to `view_->tail`
class DeltaVersionEditAcceptor
{
public:
    explicit DeltaVersionEditAcceptor(const PageEntriesView * view_, //
                                      bool                     ignore_invalid_ref_ = false,
                                      Poco::Logger *           log_                = nullptr);

    ~DeltaVersionEditAcceptor();

    void apply(PageEntriesEdit & edit);

    static void applyInplace(const PageEntriesVersionSetWithDelta::VersionPtr & current, const PageEntriesEdit & edit);

    void gcApply(PageEntriesEdit & edit) { PageEntriesBuilder::gcApplyTemplate(view, edit, current_version); }

    static void gcApplyInplace( //
        const PageEntriesVersionSetWithDelta::VersionPtr & current,
        PageEntriesEdit &                               edit)
    {
        assert(current->isBase());
        assert(current.use_count() == 1);
        PageEntriesBuilder::gcApplyTemplate(current, edit, current);
    }

private:
    // Read old state from `view` and apply new edit to `current_version`

    void applyPut(PageEntriesEdit::EditRecord & record);
    void applyDel(PageEntriesEdit::EditRecord & record);
    void applyRef(PageEntriesEdit::EditRecord & record);
    void decreasePageRef(PageId page_id);

private:
    PageEntriesView *                      view;
    PageEntriesVersionSetWithDelta::VersionPtr current_version;
    bool                                    ignore_invalid_ref;
    Poco::Logger *                          log;
};

} // namespace DB
