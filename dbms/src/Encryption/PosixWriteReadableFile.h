#pragma once

#include <Common/CurrentMetrics.h>
#include <Encryption/RateLimiter.h>
#include <Encryption/WriteReadableFile.h>
#include <common/types.h>

namespace CurrentMetrics
{
extern const Metric OpenFileForReadWrite;
} // namespace CurrentMetrics

namespace DB
{
class PosixWriteReadableFile : public WriteReadableFile
{
public:
    PosixWriteReadableFile(
        const String & file_name_,
        bool truncate_when_exists_,
        int flags,
        mode_t mode,
        const WriteLimiterPtr & write_limiter_ = nullptr,
        const ReadLimiterPtr & read_limiter_ = nullptr);

    ~PosixWriteReadableFile() override;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    int getFd() const override
    {
        return fd;
    }

    bool isClosed() const override
    {
        return fd == -1;
    }

    int fsync() override;

    int ftruncate(off_t length) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    String getFileName() const override
    {
        return file_name;
    }

    void close() override;

private:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForReadWrite, 0};

    String file_name;
    int fd;
    WriteLimiterPtr write_limiter;
    ReadLimiterPtr read_limiter;
};
} // namespace DB