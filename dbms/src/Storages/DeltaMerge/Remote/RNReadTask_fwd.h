#pragma once

#include <boost/noncopyable.hpp>
#include <memory>

namespace DB::DM::Remote
{

class RNReadTask;
using RNReadTaskPtr = std::shared_ptr<RNReadTask>;

class RNReadSegmentTask;
using RNReadSegmentTaskPtr = std::shared_ptr<RNReadSegmentTask>;

} // namespace DB::DM::Remote
