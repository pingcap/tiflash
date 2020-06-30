//
// Created by linkmyth on 2020-06-28.
//

#include <IO/EncryptedFileProvider.h>
#include <IO/EncryptedRandomAccessFile.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/EncryptedWritableFile.h>
#include <IO/PosixWritableFile.h>

namespace DB {
RandomAccessFilePtr
EncryptedFileProvider::NewRandomAccessFileImpl(const std::string &file_name_, int flags) {
    RandomAccessFilePtr underlying = std::make_shared<PosixRandomAccessFile>(file_name_, flags);
    return std::make_shared<EncryptedRandomAccessFile>(underlying);
}

WritableFilePtr EncryptedFileProvider::NewWritableFileImpl(const std::string &file_name_, int flags, mode_t mode) {
    WritableFilePtr underlying = std::make_shared<PosixWritableFile>(file_name_, flags, mode);
    return std::make_shared<EncryptedWritableFile>(underlying);
}
}
