#include <common/types.h>

namespace DB::Remote
{
struct RemoteDataLocation
{
    std::shared_ptr<const String> remote_key;

    UInt64 offset_in_file;
    UInt64 size_in_file;
};

} // namespace DB::PS::V3::Remote
