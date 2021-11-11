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
        , stream{std::move(stream_)} {};

    ~EncryptedWriteReadableFile() override;

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

    int getFd() const override
    {
        return file->getFd();
    }

    bool isClosed() const override
    {
        return file->isClosed();
    }

    void hardLink(const String & existing_file) override
    {
        file->hardLink(existing_file);
    }

    String getFileName() const
    {
        return file->getFileName();
    };

private:
    WriteReadableFilePtr file;
    BlockAccessCipherStreamPtr stream;
};

} // namespace DB