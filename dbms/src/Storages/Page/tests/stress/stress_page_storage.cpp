#include "PSBackground.h"
#include "PSRunnable.h"
#include "PSStressEnv.h"
#include "PSWorkload.h"

namespace DB
{
#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif
} // namespace DB

int main(int argc, char ** argv)
try
{
    StressEnv::initGlobalLogger();
    auto env = StressEnv::parse(argc, argv);
    env.setup();

    auto & mamager = StressWorkloadManger::getInstance();
    mamager.setEnv(env);
    mamager.runWorkload();

    return StressEnvStatus::getInstance().statrc();
}
catch (...)
{
    DB::tryLogCurrentException("");
    exit(-1);
}
