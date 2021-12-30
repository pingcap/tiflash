#pragma once

#include <memory>

namespace DB
{
class WritableFile
{
public:
    virtual ~WritableFile() = default;

    // Write N bytes of buf to file.  Return the number written, or -1.
    virtual ssize_t write(char * buf, size_t size) = 0;

    virtual ssize_t pwrite(char * buf, size_t size, off_t offset) const = 0;

    virtual std::string getFileName() const = 0;

    virtual int getFd() const = 0;

    virtual void open() = 0;

    virtual void close() = 0;

    virtual bool isClosed() const = 0;

    virtual int fsync() = 0;

    virtual int ftruncate(off_t length) = 0;

    // Create a new hard link file for `existing_file` to this file
    // Note that it will close the file descriptor if it is opened.
    virtual void hardLink(const std::string & existing_file) = 0;
};

using WritableFilePtr = std::shared_ptr<WritableFile>;

} // namespace DB
