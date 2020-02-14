#pragma once
#include <fiu-local.h>
#include <fiu-control.h>
#include <fiu.h>

#include <Core/Types.h>
#include <Common/Exception.h>

namespace DB {

#define FAIL_POINT_DEFINE(name) \
    static constexpr char name[] = #name "";

#define FAIL_POINT_ENABLE(trigger, name) \
else if (trigger == name) \
    fiu_enable(name, 1, nullptr, 0);

FAIL_POINT_DEFINE(crash_between_drop_data_and_meta)
FAIL_POINT_DEFINE(crash_between_alter_data_and_meta)

class FailPointHelper {
public:
    static void enableFailPoint (const String & fail_point_name) {
        if (false) {}
        FAIL_POINT_ENABLE(fail_point_name, crash_between_alter_data_and_meta)
        FAIL_POINT_ENABLE(fail_point_name, crash_between_drop_data_and_meta)
        else
            throw Exception("Cannot find fail point " + fail_point_name);
    }
};
}