#pragma once
#include <Poco/ArchiveStrategy.h>
namespace Poco
{
template <class DT>
class TiFlashArchiveByTimestampsStrategy : public ArchiveByTimestampStrategy<DT>
{
public:
    inline static const std::string suffix_fmt = "%Y-%m-%d-%H:%M:%S.%i";
    LogFile * archive(LogFile * p_file) override
    {
        std::string path = p_file->path();
        delete p_file;
        std::string archPath = path;
        archPath.append(".");
        DateTimeFormatter::append(archPath, DT().timestamp(), suffix_fmt);

        if (this->exists(archPath))
            this->archiveByNumber(archPath);
        else
            this->moveFile(path, archPath);

        return new LogFile(path);
    }
};
} // namespace Poco