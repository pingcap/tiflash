#pragma once

#include <Core/Types.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>

namespace DB::PS::V3
{
class BlobFile : Allocator<false>
{
public:
    BlobFile(String path_, FileProviderPtr file_provider_);

    ~BlobFile();

    String getPath();

    void read(const std::vector<PageId> & page_ids, const PageHandler & handler);

    void read(const PageId & page_id, const PageHandler & handler);

    void write(WriteBatch && write_batch);

private:
};


} // namespace DB::PS::V3