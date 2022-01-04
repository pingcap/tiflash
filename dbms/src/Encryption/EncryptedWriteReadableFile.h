#pragma once

#include <Encryption/AESCTRCipherStream.h>
#include <Encryption/WriteReadableFile.h>
#include <common/types.h>

#include <string>

namespace DB
{
class EncryptedWriteReadableFile : public WriteReadableFile
{
public:
    EncryptedWriteReadableFile(WriteReadableFilePtr & file_, BlockAccessCipherStreamPtr stream_)
        : file{file_}
        , stream{std::move(stream_)}
    {}

    ~EncryptedWriteReadableFile() override = default;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    void close() override
    {
        file->close();
    }

    int fsync() override
    {
        return file->fsync();
    }

    int ftruncate(off_t length) override
    {
        return file->ftruncate(length);
    }

    int getFd() const override
    {
        return file->getFd();
    }

    bool isClosed() const override
    {
        return file->isClosed();
    }

    String getFileName() const override
    {
        return file->getFileName();
    }

private:
    WriteReadableFilePtr file;
    BlockAccessCipherStreamPtr stream;
};

} // namespace DB