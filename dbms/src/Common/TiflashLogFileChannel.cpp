#include "TiflashLogFileChannel.h"

#include <Poco/LocalDateTime.h>

#include "Poco/ArchiveStrategy.h"
#include "Poco/String.h"
#include "TiflashArchiveByTimestampsStrategy.h"
namespace DB
{
void TiflashLogFileChannel::setArchive(const std::string & archive)
{
    Poco::ArchiveStrategy * pStrategy = 0;
    if (_times == "utc")
        pStrategy = new TiflashArchiveByTimestampsStrategy<Poco::DateTime>;
    else if (_times == "local")
        pStrategy = new TiflashArchiveByTimestampsStrategy<Poco::LocalDateTime>;
    else
        throw Poco::PropertyNotSupportedException("times", _times);
    delete _pArchiveStrategy;
    pStrategy->compress(_compress);
    _pArchiveStrategy = pStrategy;
    _archive = archive;
}
} // namespace DB