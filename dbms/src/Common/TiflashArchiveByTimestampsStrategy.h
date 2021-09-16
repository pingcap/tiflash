#pragma once
#include <Poco/ArchiveStrategy.h>
namespace DB
{
template <class DT>
class TiflashArchiveByTimestampsStrategy : public Poco::ArchiveByTimestampStrategy<DT>
{
public:
    Poco::LogFile * archive(Poco::LogFile * pFile) override
    {
        std::string path = pFile->path();
        delete pFile;
        std::string archPath = path;
        archPath.append(".");
        Poco::DateTimeFormatter::append(archPath, DT().timestamp(), "%Y-%m-%d-%H:%M:%S.%i");

        if (Poco::ArchiveByTimestampStrategy<DT>::exists(archPath))
            Poco::ArchiveByTimestampStrategy<DT>::archiveByNumber(archPath);
        else
            Poco::ArchiveByTimestampStrategy<DT>::moveFile(path, archPath);

        return new Poco::LogFile(path);
    }
};
} // namespace DB