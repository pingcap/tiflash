#pragma once

#include <Common/CurrentMetrics.h>
#include <Encryption/RandomAccessFile.h>
#include <string>

namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
class PosixRandomAccessFile : public RandomAccessFile
{
protected:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

public:
    PosixRandomAccessFile(const std::string & file_name_, int flags);

    ~PosixRandomAccessFile() override;

    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file_name; }

    bool isClosed() const override { return fd == -1; }

    int getFd() const override { return fd; }

    void close() override;

private:
    std::string file_name;
    int fd;
};

} // namespace DB
