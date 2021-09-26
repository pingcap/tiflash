#include <Poco/ArchiveStrategy.h>
#include <Poco/Ext/TiFlashArchiveByTimestampsStrategy.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/LocalDateTime.h>
#include <Poco/String.h>
namespace DB
{
void TiFlashLogFileChannel::setArchive(const std::string & archive)
{
    Poco::ArchiveStrategy * pStrategy = nullptr;
    if (archive == "number")
    {
        pStrategy = new Poco::ArchiveByNumberStrategy;
    }
    else if (archive == "timestamp")
    {
        if (_times == "utc")
            pStrategy = new TiFlashArchiveByTimestampsStrategy<Poco::DateTime>;
        else if (_times == "local")
            pStrategy = new TiFlashArchiveByTimestampsStrategy<Poco::LocalDateTime>;
        else
            throw Poco::PropertyNotSupportedException("times", _times);
    }
    else
        throw Poco::InvalidArgumentException("archive", archive);
    delete _pArchiveStrategy;
    pStrategy->compress(_compress);
    _pArchiveStrategy = pStrategy;
    _archive = archive;
}
} // namespace DB