#pragma once

#include <Encryption/RandomAccessFile.h>
#include <Encryption/WritableFile.h>

#include <memory>
namespace DB
{
class WriteReadableFile : public WritableFile
    , public RandomAccessFile
{
};
} // namespace DB