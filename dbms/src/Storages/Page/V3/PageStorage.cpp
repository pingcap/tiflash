#include <Storages/Page/V3/PageStorage.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace PS::V3
{
PageStorage::PageStorage(String name,
                         PSDiskDelegatorPtr delegator_, //
                         const Config & config_,
                         const FileProviderPtr & file_provider_)
    : DB::PageStorage(name, delegator_, config_, file_provider_)
    , log(&Poco::Logger::get("PageStorage")){};

void PageStorage::restore()
{
    throw Exception("TBD", ErrorCodes::NOT_IMPLEMENTED);
}

std::set<BlobFile> PageStorage::listAllPageFiles(const FileProviderPtr & file_provider,
                                                 PSDiskDelegatorPtr & delegator,
                                                 Poco::Logger * page_file_log)
{
    std::vector<std::pair<String, Strings>> all_file_names;
    {
        std::vector<std::string> file_names;
        for (const auto & p : delegator->listPaths())
        {
            Poco::File directory(p);
            if (!directory.exists())
                directory.createDirectories();
            // file_names.clear();
            directory.list(file_names);
            all_file_names.emplace_back(std::make_pair(p, std::move(file_names)));
            // file_names.clear();
        }
    }

    if (all_file_names.empty())
        return {};

    std::set<BlobFile> blob_files;

    for (const auto & [directory, names] : all_file_names)
    {
        for (const auto & name : names)
        {
            // TBD
        }
    }
    // TBD
    return {};
}

} // namespace PS::V3
} // namespace DB
