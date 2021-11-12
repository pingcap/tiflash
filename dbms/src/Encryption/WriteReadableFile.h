#pragma once

#include <Encryption/RandomAccessFile.h>
#include <Encryption/WritableFile.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class WriteReadableFile
{
public:
    virtual ~WriteReadableFile() = default;

    virtual ssize_t pwrite(char * buf, size_t size, off_t offset) const = 0;

    virtual ssize_t pread(char * buf, size_t size, off_t offset) const = 0;

    virtual int fsync() = 0;

    virtual int getFd() const = 0;

    virtual bool isClosed() const = 0;

    virtual void close() = 0;

    virtual void hardLink(const String & existing_file) = 0;

    virtual String getFileName() const = 0;
};

using WriteReadableFilePtr = std::shared_ptr<WriteReadableFile>;

} // namespace DB