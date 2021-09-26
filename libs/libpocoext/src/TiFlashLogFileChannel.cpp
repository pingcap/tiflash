#include <Poco/ArchiveStrategy.h>
#include <Poco/Ext/TiFlashArchiveByTimestampsStrategy.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/LocalDateTime.h>
#include <Poco/String.h>
namespace Poco
{
void TiFlashLogFileChannel::setArchive(const std::string & archive)
{
    ArchiveStrategy * pStrategy = nullptr;
    if (archive == "number")
    {
        pStrategy = new ArchiveByNumberStrategy;
    }
    else if (archive == "timestamp")
    {
        if (_times == "utc")
            pStrategy = new TiFlashArchiveByTimestampsStrategy<DateTime>;
        else if (_times == "local")
            pStrategy = new TiFlashArchiveByTimestampsStrategy<LocalDateTime>;
        else
            throw PropertyNotSupportedException("times", _times);
    }
    else
        throw InvalidArgumentException("archive", archive);
    delete _pArchiveStrategy;
    pStrategy->compress(_compress);
    _pArchiveStrategy = pStrategy;
    _archive = archive;
}
} // namespace Poco