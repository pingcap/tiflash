#pragma once

#include <Encryption/AESCTRCipherStream.h>
#include <IO/WritableFile.h>
#include <string>

namespace DB
{
class EncryptedWritableFile : public WritableFile
{
public:
    EncryptedWritableFile(WritableFilePtr & file_, BlockAccessCipherStreamPtr stream_)
        : file{file_}, file_offset{0}, stream{std::move(stream_)}
    {}

    ~EncryptedWritableFile() override = default;

    ssize_t write(char * buf, size_t size) override;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFd() const override { return file->getFd(); }

    void open() override;

    void close() override;

    bool isClosed() override { return file->isClosed(); }

    int fsync() override { return file->fsync(); }

private:
    WritableFilePtr file;

    off_t file_offset;

    BlockAccessCipherStreamPtr stream;
};

} // namespace DB
