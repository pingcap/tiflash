#include <utility>

#pragma once

#include <Storages/Page/stable/PageEntries.h>
#include <Storages/Page/stable/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/stable/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/stable/VersionSet/PageEntriesView.h>
#include <Storages/Page/stable/mvcc/VersionSet.h>
#include <Storages/Page/stable/mvcc/VersionSetWithDelta.h>

namespace DB::stable
{

class DeltaVersionEditAcceptor;

class PageEntriesVersionSetWithDelta : public DB::stable::MVCC::VersionSetWithDelta< //
                                           PageEntriesForDelta,
                                           PageEntriesView,
                                           PageEntriesEdit,
                                           DeltaVersionEditAcceptor>
{
public:
    using BaseType     = DB::stable::MVCC::VersionSetWithDelta<PageEntriesForDelta, PageEntriesView, PageEntriesEdit, DeltaVersionEditAcceptor>;
    using EditAcceptor = BaseType::EditAcceptor;
    using VersionType  = BaseType::VersionType;
    using VersionPtr   = BaseType::VersionPtr;

public:
    explicit PageEntriesVersionSetWithDelta(const DB::stable::MVCC::VersionSetConfig & config_, Poco::Logger * log_) : BaseType(config_, log_) {}

public:
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> gcApply(PageEntriesEdit & edit, bool need_scan_page_ids = true);

    /// List all PageFile that are used by any version
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> listAllLiveFiles(const std::unique_lock<std::shared_mutex> &,
                                                                               bool need_scan_page_ids = true) const;

private:
    void collectLiveFilesFromVersionList(const PageEntriesView &        view,
                                         std::set<PageFileIdAndLevel> & live_files,
                                         std::set<PageId> &             live_normal_pages,
                                         bool                           need_scan_page_ids) const;
};

/// Read old entries state from `view_` and apply new edit to `view_->tail`
class DeltaVersionEditAcceptor
{
public:
    explicit DeltaVersionEditAcceptor(const PageEntriesView * view_, //
                                      bool                    ignore_invalid_ref_ = false,
                                      Poco::Logger *          log_                = nullptr);

    ~DeltaVersionEditAcceptor();

    void apply(PageEntriesEdit & edit);

    static void applyInplace(const PageEntriesVersionSetWithDelta::VersionPtr & current, const PageEntriesEdit & edit, Poco::Logger * log);

    void gcApply(PageEntriesEdit & edit) { PageEntriesBuilder::gcApplyTemplate(view, edit, current_version); }

    static void gcApplyInplace( //
        const PageEntriesVersionSetWithDelta::VersionPtr & current,
        PageEntriesEdit &                                  edit)
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
    PageEntriesView *                          view;
    PageEntriesVersionSetWithDelta::VersionPtr current_version;
    bool                                       ignore_invalid_ref;
    Poco::Logger *                             log;
};

} // namespace DB::stable
