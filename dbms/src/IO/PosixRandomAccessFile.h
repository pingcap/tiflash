#pragma once

#include <string>
#include <IO/RandomAccessFile.h>


namespace DB {
    class PosixRandomAccessFile : public RandomAccessFile {
    public:
        PosixRandomAccessFile(const std::string &file_name_, int flags);

        ~PosixRandomAccessFile() override;

        ssize_t read(char *buf, size_t size) const override;

        ssize_t pread(char *buf, size_t size, off_t offset) const override;

        std::string getFileName() const override {
            return file_name;
        }

        int getFd() const override {
            return fd;
        }

        void close() override;

    private:
        std::string file_name;
        int fd;
    };

}
