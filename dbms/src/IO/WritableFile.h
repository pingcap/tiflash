#pragma once

#include <memory>

namespace DB
{
class WritableFile
{
public:
    WritableFile() = default;

    virtual ~WritableFile() = default;

    virtual ssize_t write(char * buf, size_t size) = 0;

    virtual std::string getFileName() const = 0;

    virtual int getFd() const = 0;

    virtual void close() = 0;
};

using WritableFilePtr = std::shared_ptr<WritableFile>;

} // namespace DB
