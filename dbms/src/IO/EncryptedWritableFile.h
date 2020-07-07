#pragma once

#include <IO/WritableFile.h>
#include <string>
#include <Encryption/AESCTRCipherStream.h>

namespace DB
{
class EncryptedWritableFile : public WritableFile
{
public:
    EncryptedWritableFile(WritableFilePtr & file_, BlockAccessCipherStreamPtr stream_)
    : file{file_}, file_offset{0}, stream{std::move(stream_)} {}

    ~EncryptedWritableFile() override = default;

    ssize_t write(char * buf, size_t size) override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFd() const override { return file->getFd(); }

    void close() override;

private:
    WritableFilePtr file;

    off_t file_offset;

    BlockAccessCipherStreamPtr stream;
};

} // namespace DB
