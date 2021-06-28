#pragma once

#include <Storages/Page/PageEntries.h>
#include <Storages/Page/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/VersionSet/PageEntriesView.h>
#include <Storages/Page/mvcc/VersionSet.h>
#include <Storages/Page/mvcc/VersionSetWithDelta.h>

#include <utility>

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
    explicit PageEntriesVersionSetWithDelta(String name_, const ::DB::MVCC::VersionSetConfig & config_, Poco::Logger * log_)
        : BaseType(std::move(name_), config_, log_)
    {
    }

public:
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> gcApply(PageEntriesEdit & edit, bool need_scan_page_ids = true);

    /// List all PageFile that are used by any version
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> //
    listAllLiveFiles(std::unique_lock<std::shared_mutex> &&, bool need_scan_page_ids = true);

private:
    void collectLiveFilesFromVersionList( //
        const PageEntriesView &        view,
        std::set<PageFileIdAndLevel> & live_files,
        std::set<PageId> &             live_normal_pages,
        bool                           need_scan_page_ids) const;
};

/// Read old entries state from `view_` and apply new edit to `view_->tail`
class DeltaVersionEditAcceptor
{
public:
    explicit DeltaVersionEditAcceptor(const PageEntriesView * view_, //
                                      const String &          name_,
                                      bool                    ignore_invalid_ref_ = false,
                                      Poco::Logger *          log_                = nullptr);

    ~DeltaVersionEditAcceptor();

    void apply(PageEntriesEdit & edit);

    static void applyInplace(const String &                                     name,
                             const PageEntriesVersionSetWithDelta::VersionPtr & current,
                             const PageEntriesEdit &                            edit,
                             Poco::Logger *                                     log);

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

    const String & name;
    Poco::Logger * log;
};

} // namespace DB
