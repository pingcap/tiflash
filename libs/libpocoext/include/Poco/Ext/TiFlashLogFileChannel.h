#pragma once
#include <Poco/FileChannel.h>
namespace DB
{
class TiFlashLogFileChannel : public Poco::FileChannel
{
protected:
    void setArchive(const std::string & archive) override;
};
} // namespace DB