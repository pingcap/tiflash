#pragma once

#include <Common/CurrentMetrics.h>
#include <Encryption/RateLimiter.h>
#include <Encryption/WriteReadableFile.h>
#include <common/types.h>

namespace CurrentMetrics
{
// TODO : replace it with new metrics which named OpenFile
extern const Metric OpenFileForWrite;
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

    void hardLink(const String & existing_file) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    String getFileName() const override
    {
        return file_name;
    }

    void close() override;

protected:
    // Only add metrics when file is actually added in `doOpenFile`.
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite, 0};

private:
    String file_name;
    int fd;
    WriteLimiterPtr write_limiter;
    ReadLimiterPtr read_limiter;
};
} // namespace DB