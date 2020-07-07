#pragma once

#include <IO/FileProvider.h>

namespace DB
{
class PosixFileProvider : public FileProvider
{
protected:
    RandomAccessFilePtr newRandomAccessFileImpl(const std::string & file_name_, int flags) override;

    WritableFilePtr newWritableFileImpl(const std::string & file_name_, int flags, mode_t mode) override;

public:
    PosixFileProvider() = default;
    ~PosixFileProvider() override = default;
};
} // namespace DB
