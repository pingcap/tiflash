#pragma once

#include <Encryption/AESCTRCipherStream.h>
#include <Encryption/RandomAccessFile.h>
#include <string>

namespace DB
{
class EncryptedRandomAccessFile : public RandomAccessFile
{
public:
    EncryptedRandomAccessFile(RandomAccessFilePtr & file_, BlockAccessCipherStreamPtr stream_)
        : file{file_}, file_offset{0}, stream{std::move(stream_)}
    {}

    ~EncryptedRandomAccessFile() override = default;

    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFd() const override { return file->getFd(); }

    bool isClosed() const override { return file->isClosed(); }

    void close() override;

private:
    RandomAccessFilePtr file;

    off_t file_offset;

    BlockAccessCipherStreamPtr stream;
};

} // namespace DB
