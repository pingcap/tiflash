//
// Created by linkmyth on 2020-06-28.
//

#include <IO/PosixFileProvider.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/PosixWritableFile.h>

namespace DB {
    RandomAccessFilePtr
    PosixFileProvider::NewRandomAccessFileImpl(const std::string &file_name_, int flags) {
        return std::make_shared<PosixRandomAccessFile>(file_name_, flags);
    }

    WritableFilePtr PosixFileProvider::NewWritableFileImpl(const std::string &file_name_, int flags, mode_t mode) {
        return std::make_shared<PosixWritableFile>(file_name_, flags, mode);
    }
}
