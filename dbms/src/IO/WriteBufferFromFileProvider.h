#pragma once

#include <IO/FileProvider.h>
#include <IO/WritableFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>

namespace DB
{
class WriteBufferFromFileProvider : public WriteBufferFromFileDescriptor
{
protected:
    void nextImpl() override;

public:
    WriteBufferFromFileProvider(FileProviderPtr & file_provider_,
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromFileProvider() override = default;

    void close();

    std::string getFileName() const override { return file->getFileName(); }

    int getFD() const override { return file->getFd(); }

private:
    WritableFilePtr file;
};
} // namespace DB
