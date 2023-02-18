#pragma once

#include <Storages/Page/V3/PageDefines.h>
#include <common/types.h>
#include <Poco/Message.h>

namespace DB::PS::V3
{

struct BlobFileGCInfo
{
    BlobFileId blob_id;
    double valid_rate;
};

struct BlobFileTruncateInfo
{
    BlobFileId blob_id;
    UInt64 origin_size;
    UInt64 truncated_size;
    double valid_rate;
};

struct BlobStoreGCInfo
{
    enum Type
    {
        ReadOnly = 0,
        Unchanged = 1,
        FullGC = 2,
        Truncated = 3,
    };

    Poco::Message::Priority getLoggingLevel() const;

    String toString() const;

    void appendToReadOnlyBlob(const BlobFileId blob_id, double valid_rate)
    {
        blob_gc_info[ReadOnly].emplace_back(BlobFileGCInfo{blob_id, valid_rate});
    }

    void appendToNoNeedGCBlob(const BlobFileId blob_id, double valid_rate)
    {
        blob_gc_info[Unchanged].emplace_back(BlobFileGCInfo{blob_id, valid_rate});
    }

    void appendToNeedGCBlob(const BlobFileId blob_id, double valid_rate)
    {
        blob_gc_info[FullGC].emplace_back(BlobFileGCInfo{blob_id, valid_rate});
    }

    void appendToTruncatedBlob(const BlobFileId blob_id, UInt64 origin_size, UInt64 truncated_size, double valid_rate)
    {
        blob_gc_truncate_info.emplace_back(BlobFileTruncateInfo{blob_id, origin_size, truncated_size, valid_rate});
    }

private:
    // 1. read only blob
    // 2. no need gc blob
    // 3. full gc blob
    std::vector<BlobFileGCInfo> blob_gc_info[3];

    std::vector<BlobFileTruncateInfo> blob_gc_truncate_info;

    String toTypeString(Type type_index) const;

    String toTypeTruncateString(Type type_index) const;
};

} // namespace DB::PS::V3

