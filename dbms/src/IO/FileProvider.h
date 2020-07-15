#pragma once

#include <IO/RandomAccessFile.h>
#include <IO/WritableFile.h>

namespace DB
{
class FileProvider
{
protected:
    virtual RandomAccessFilePtr newRandomAccessFileImpl(const std::string & file_name_, int flags) = 0;

    virtual WritableFilePtr newWritableFileImpl(const std::string & file_name_, int flags, mode_t mode) = 0;

public:
    RandomAccessFilePtr newRandomAccessFile(const std::string & file_name_, int flags = -1)
    {
        return newRandomAccessFileImpl(file_name_, flags);
    }

    WritableFilePtr newWritableFile(const std::string & file_name_, int flags = -1, mode_t mode = 0666)
    {
        return newWritableFileImpl(file_name_, flags, mode);
    }

    virtual void renameFile(const std::string & src_fname, const std::string & dst_fname) = 0;

    virtual ~FileProvider() = default;
};

using FileProviderPtr = std::shared_ptr<FileProvider>;
} // namespace DB

