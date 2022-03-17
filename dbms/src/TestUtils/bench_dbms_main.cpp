#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>

// main function for bench_xxx.cpp.
int main(int argc, char * argv[])
{
    benchmark::Initialize(&argc, argv);
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();
    if (::benchmark::ReportUnrecognizedArguments(argc, argv))
        return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    DB::tests::TiFlashTestEnv::shutdown();
    ::benchmark::Shutdown();
    return 0;
}
