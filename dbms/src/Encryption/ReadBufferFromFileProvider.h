#pragma once

#include <Common/CurrentMetrics.h>
#include <Encryption/FileProvider.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileDescriptor.h>

namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
class ReadBufferFromFileProvider : public ReadBufferFromFileDescriptor
{
protected:
    bool nextImpl() override;

public:
    ReadBufferFromFileProvider(const FileProviderPtr & file_provider_,
        const std::string & file_name_,
        const EncryptionPath & encryption_path_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ReadBufferFromFileProvider(ReadBufferFromFileProvider &&) = default;

    ~ReadBufferFromFileProvider() override;

    void close();

    std::string getFileName() const override { return file->getFileName(); }

    int getFD() const override { return file->getFd(); }

private:
    off_t doSeekInFile(off_t offset, int whence) override;

private:
    RandomAccessFilePtr file;
};
} // namespace DB
