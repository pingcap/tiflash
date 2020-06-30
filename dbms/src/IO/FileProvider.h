#pragma once

#include <IO/WritableFile.h>
#include <IO/RandomAccessFile.h>

namespace DB {
    class FileProvider {
    protected:
        virtual RandomAccessFilePtr NewRandomAccessFileImpl(
                const std::string &file_name_,
                int flags) = 0;

        virtual WritableFilePtr NewWritableFileImpl(
                const std::string &file_name_,
                int flags,
                mode_t mode) = 0;

    public:
        RandomAccessFilePtr NewRandomAccessFile(
                const std::string &file_name_,
                int flags = -1) {
            return NewRandomAccessFileImpl(file_name_, flags);
        }

        WritableFilePtr NewWritableFile(
                const std::string &file_name_,
                int flags = -1,
                mode_t mode = 0666) {
            return NewWritableFileImpl(file_name_, flags, mode);
        };

        virtual ~FileProvider() = 0;
    };

    using FileProviderPtr = std::shared_ptr<FileProvider>;
}
