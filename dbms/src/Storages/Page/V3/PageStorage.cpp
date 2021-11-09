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
                        , log(&Poco::Logger::get("PageStorage"))
                        {};

void PageStorage::restore()
{
    throw Exception("TBD",ErrorCodes::NOT_IMPLEMENTED);
}

} // namespace PS::V3
} // namespace DB
