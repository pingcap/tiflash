#pragma once

#include <IO/RandomAccessFile.h>
#include <string>


namespace DB
{
class EncryptedRandomAccessFile : public RandomAccessFile
{
public:
    EncryptedRandomAccessFile(RandomAccessFilePtr & file_) : file{file_} {}

    ~EncryptedRandomAccessFile() override = default;

    ssize_t read(char * buf, size_t size) const override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFd() const override { return file->getFd(); }

    void close() override;

private:
    RandomAccessFilePtr file;
};

} // namespace DB