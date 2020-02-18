#pragma once
#include <Common/Exception.h>
#include <Core/Types.h>
#include <fiu-control.h>
#include <fiu-local.h>
#include <fiu.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
};

#define FAIL_POINT_REGISTER(name) static constexpr char name[] = #name "";

#define FAIL_POINT_ENABLE(trigger, name) else if (trigger == name) fiu_enable(name, 1, nullptr, FIU_ONETIME);

FAIL_POINT_REGISTER(exception_between_drop_data_and_meta)
FAIL_POINT_REGISTER(exception_between_alter_data_and_meta)
FAIL_POINT_REGISTER(exception_drop_table_during_remove_meta)

#define FAIL_POINT_TRIGGER_EXCEPTION(fail_point) \
    fiu_do_on(fail_point, throw Exception("Fail point " #fail_point " is triggered.", ErrorCodes::FAIL_POINT_ERROR);)

class FailPointHelper
{
public:
    static void enableFailPoint(const String & fail_point_name)
    {
        if (false) {}
        FAIL_POINT_ENABLE(fail_point_name, exception_between_alter_data_and_meta)
        FAIL_POINT_ENABLE(fail_point_name, exception_between_drop_data_and_meta)
        FAIL_POINT_ENABLE(fail_point_name, exception_drop_table_during_remove_meta)
        else throw Exception("Cannot find fail point " + fail_point_name, ErrorCodes::FAIL_POINT_ERROR);
    }
};
} // namespace DB