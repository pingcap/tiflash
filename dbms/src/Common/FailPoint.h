#pragma once
#include <Common/Exception.h>
#include <Core/Types.h>
#include <fiu-control.h>
#include <fiu-local.h>
#include <fiu.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
};

/// Macros to set failpoints.
// When `fail_point` is enabled, throw an exception
#define FAIL_POINT_TRIGGER_EXCEPTION(fail_point) \
    fiu_do_on(fail_point, throw Exception("Fail point " #fail_point " is triggered.", ErrorCodes::FAIL_POINT_ERROR);)
// When `fail_point` is enabled, wait till it is disabled
#define FAIL_POINT_PAUSE(fail_point) fiu_do_on(fail_point, FailPointHelper::wait(fail_point);)


class FailPointChannel;
class FailPointHelper
{
public:
    static void enableFailPoint(const String & fail_point_name);

    static void disableFailPoint(const String & fail_point_name);

    static void wait(const String & fail_point_name);

private:
    static std::unordered_map<String, std::shared_ptr<FailPointChannel>> fail_point_wait_channels;
};
} // namespace DB
