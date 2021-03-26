#pragma once

#include <Common/CurrentMetrics.h>
#include <Encryption/RateLimiter.h>
#include <Encryption/WritableFile.h>

#include <string>

namespace CurrentMetrics
{
extern const Metric OpenFileForWrite;
}

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{
class PosixWritableFile : public WritableFile
{
protected:
    // Only add metrics when file is actually added in `doOpenFile`.
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite, 0};

public:
    PosixWritableFile(
        const std::string & file_name_, bool truncate_when_exists_, int flags, mode_t mode, const RateLimiterPtr & rate_limiter_ = nullptr);

    ~PosixWritableFile() override;

    ssize_t write(char * buf, size_t size) override;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file_name; }

    int getFd() const override { return fd; }

    void open() override;

    void close() override;

    bool isClosed() override { return fd == -1; }

    int fsync() override;

private:
    void doOpenFile(bool truncate_when_exists_, int flags, mode_t mode);

private:
    std::string file_name;
    int fd;
    RateLimiterPtr rate_limiter;
};

} // namespace DB
