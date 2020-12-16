#pragma once

#include <Encryption/FileProvider.h>
#include <Encryption/WritableFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>

namespace DB
{

class WriteBufferFromFileProvider : public WriteBufferFromFileDescriptor
{
protected:
    void nextImpl() override;

public:
    WriteBufferFromFileProvider(const FileProviderPtr & file_provider_,
        const std::string & file_name_,
        const EncryptionPath & encryption_path,
        bool create_new_encryption_info_ = true,
        const RateLimiterPtr & rate_limiter_ = nullptr,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromFileProvider() override;

    void close();

    std::string getFileName() const override { return file->getFileName(); }

    int getFD() const override { return file->getFd(); }

private:
    WritableFilePtr file;
};
} // namespace DB
