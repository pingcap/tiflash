// Copyright 2022 PingCAP, Ltd.
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

#include <PSStressEnv.h>
#include <PSWorkload.h>

namespace DB
{
// Define is_background_thread for this binary
// It is required for `RateLimiter` but we do not link with `BackgroundProcessingPool`.
#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif

namespace PS::tests
{
int StressWorkload::main(int argc, char ** argv)
try
{
    StressEnv::initGlobalLogger();
    auto env = StressEnv::parse(argc, argv);
    env.setup();

    auto & mamager = StressWorkloadManger::getInstance();
    mamager.setEnv(env);
    mamager.runWorkload();

    return StressEnvStatus::getInstance().isSuccess();
}
catch (...)
{
    DB::tryLogCurrentException("");
    exit(-1);
}
} // namespace PS::tests
} // namespace DB
