#pragma once

#include <IO/WritableFile.h>
#include <string>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{
class PosixWritableFile : public WritableFile
{
public:
    PosixWritableFile(const std::string & file_name_, int flags, mode_t mode);

    ~PosixWritableFile() override;

    ssize_t write(char * buf, size_t size) override;

    ssize_t pwrite(char *buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file_name; }

    int getFd() const override { return fd; }

    void close() override;

    bool isClosed() override { return fd == -1; }

private:
    std::string file_name;
    int fd;
};

} // namespace DB
