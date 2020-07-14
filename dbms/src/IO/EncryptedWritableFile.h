#pragma once

#include <IO/WritableFile.h>
#include <string>

namespace DB
{
class EncryptedWritableFile : public WritableFile
{
public:
    EncryptedWritableFile(WritableFilePtr & file_) : file{file_} {}

    ~EncryptedWritableFile() override = default;

    ssize_t write(const char * buf, size_t size) const override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFd() const override { return file->getFd(); }

    void close() override;

private:
    WritableFilePtr file;
};

} // namespace DB
