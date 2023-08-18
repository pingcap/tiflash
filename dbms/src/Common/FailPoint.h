// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Types.h>
#include <fiu-control.h>
#include <fiu-local.h>
#include <fiu.h>

#include <any>
#include <unordered_map>

namespace Poco
{
namespace Util
{
class LayeredConfiguration;
}
} // namespace Poco

namespace DB
{
namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
};

#ifndef NDEBUG
/// Macros to set failpoints.
// When `fail_point` is enabled, throw an exception
#define FAIL_POINT_TRIGGER_EXCEPTION(fail_point) \
    fiu_do_on(fail_point, throw Exception("Fail point " #fail_point " is triggered.", ErrorCodes::FAIL_POINT_ERROR);)
// When `fail_point` is enabled, wait till it is disabled
#define FAIL_POINT_PAUSE(fail_point) fiu_do_on(fail_point, FailPointHelper::wait(fail_point);)
#else
#define FAIL_POINT_TRIGGER_EXCEPTION(fail_point) ;
#define FAIL_POINT_PAUSE(fail_point) ;
#endif

class FailPointChannel;
class FailPointHelper
{
public:
    static void enableFailPoint(const String & fail_point_name, std::optional<std::any> v = std::nullopt);

    static std::optional<std::any> getFailPointVal(const String & fail_point_name);

    static void enablePauseFailPoint(const String & fail_point_name, UInt64 time);

    static void disableFailPoint(const String & fail_point_name);

    static void wait(const String & fail_point_name);

    /*
     * For Server RandomFailPoint test usage. When FIU_ENABLE is defined, this function does the following work:
     * 1. Return if TiFlash config has empty flash.random_fail_points cfg
     * 2. Parse flash.random_fail_points, which expect to has "FailPointA-RatioA,FailPointB-RatioB,..." format
     * 3. Call enableRandomFailPoint method with parsed FailPointName and Rate
     */
    static void initRandomFailPoints(Poco::Util::LayeredConfiguration & config, const LoggerPtr & log);

    /*
     * For Server RandomFailPoint test usage. When FIU_ENABLE is defined, this function does the following work:
     * 1. Return if TiFlash config has empty flash.random_fail_points cfg
     * 2. Parse flash.random_fail_points, which expect to has "FailPointA-RatioA,FailPointB-RatioB,..." format
     * 3. Call disableFailPoint method with parsed FailPointName and ignore Rate.
     */
    static void disableRandomFailPoints(Poco::Util::LayeredConfiguration & config, const LoggerPtr & log);

    static void enableRandomFailPoint(const String & fail_point_name, double rate);

private:
#ifdef FIU_ENABLE
    static std::unordered_map<String, std::shared_ptr<FailPointChannel>> fail_point_wait_channels;
    static std::unordered_map<String, std::any> fail_point_val;
#endif
};
} // namespace DB
