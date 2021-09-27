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
        std::string arch_path = path;
        arch_path.append(".");
        DateTimeFormatter::append(arch_path, DT().timestamp(), suffix_fmt);

        if (this->exists(arch_path))
            this->archiveByNumber(arch_path);
        else
            this->moveFile(path, arch_path);

        return new LogFile(path);
    }
};
} // namespace Poco
