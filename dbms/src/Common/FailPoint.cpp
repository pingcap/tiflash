#include <Common/FailPoint.h>

namespace DB
{
std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointHelper::fail_point_wait_channels;

} // namespace DB
