#pragma once
#include <Poco/FileChannel.h>
namespace Poco
{
class TiFlashLogFileChannel : public FileChannel
{
protected:
    void setArchive(const std::string & archive) override;
};
} // namespace DB