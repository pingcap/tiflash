#pragma once

#include <Core/Types.h>
#include <Encryption/FileProvider.h>
#include <Encryption/WriteReadableFile.h>
#include <Poco/Logger.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>

namespace DB::PS::V3
{
class BlobFile
{
public:
    BlobFile(String path_,
             FileProviderPtr file_provider_);

    ~BlobFile();

    String getPath()
    {
        return path;
    }

    EncryptionPath getEncryptionPath()
    {
        return EncryptionPath(getPath(), "");
    }

    BlobFileId getBlobFileId();

    void read(char * buffer, size_t offset, size_t size, const ReadLimiterPtr & read_limiter);

    void write(char * buffer, size_t offset, size_t size, const WriteLimiterPtr & write_limiter);

    void truncate(size_t size);

    void remove();

private:
    FileProviderPtr file_provider;
    String path;

    WriteReadableFilePtr wrfile;
};
using BlobFilePtr = std::shared_ptr<BlobFile>;

} // namespace DB::PS::V3