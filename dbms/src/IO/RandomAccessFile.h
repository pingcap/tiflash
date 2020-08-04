#pragma once

#include <memory>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{

class RandomAccessFile
{
public:
    virtual ~RandomAccessFile() = default;

    virtual off_t seek(off_t offset, int whence) = 0;

    virtual ssize_t read(char * buf, size_t size) = 0;

    virtual ssize_t pread(char * buf, size_t size, off_t offset) const = 0;

    virtual std::string getFileName() const = 0;

    virtual int getFd() const = 0;

    virtual bool isClosed() const = 0;

    virtual void close() = 0;
};

using RandomAccessFilePtr = std::shared_ptr<RandomAccessFile>;
} // namespace DB
