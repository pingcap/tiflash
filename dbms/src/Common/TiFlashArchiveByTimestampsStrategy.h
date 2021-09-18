#pragma once
#include <Poco/ArchiveStrategy.h>
namespace DB
{
template <class DT>
class TiFlashArchiveByTimestampsStrategy : public Poco::ArchiveByTimestampStrategy<DT>
{
public:
    inline static const std::string suffix_fmt = "%Y-%m-%d-%H:%M:%S.%i";
    Poco::LogFile * archive(Poco::LogFile * pFile) override
    {
        std::string path = pFile->path();
        delete pFile;
        std::string archPath = path;
        archPath.append(".");
        Poco::DateTimeFormatter::append(archPath, DT().timestamp(), suffix_fmt);

        if (this->exists(archPath))
            this->archiveByNumber(archPath);
        else
            this->moveFile(path, archPath);

        return new Poco::LogFile(path);
    }
};
} // namespace DB