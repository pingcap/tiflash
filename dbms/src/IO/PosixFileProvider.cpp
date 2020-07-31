#include <IO/PosixFileProvider.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/PosixWritableFile.h>
#include <Poco/File.h>

namespace DB
{
RandomAccessFilePtr PosixFileProvider::newRandomAccessFileImpl(
    const std::string & file_path_, const EncryptionPath & encryption_path_, int flags) const
{
    std::ignore = encryption_path_;
    return std::make_shared<PosixRandomAccessFile>(file_path_, flags);
}

WritableFilePtr PosixFileProvider::newWritableFileImpl(const std::string & file_path_, const EncryptionPath & encryption_path_,
    bool create_new_file_, bool create_new_encryption_info_, int flags, mode_t mode) const
{
    std::ignore = create_new_encryption_info_;
    std::ignore = encryption_path_;
    return std::make_shared<PosixWritableFile>(file_path_, create_new_file_, flags, mode);
}

void PosixFileProvider::deleteFile(const std::string & file_path_, const EncryptionPath & encryption_path_) const
{
    std::ignore = encryption_path_;
    Poco::File data_file(file_path_);
    if (data_file.exists())
    {
        data_file.remove();
    }
}

void PosixFileProvider::createEncryptionInfo(const std::string & file_path_) const { std::ignore = file_path_; }

} // namespace DB
