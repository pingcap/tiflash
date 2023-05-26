#include <Common/FailPoint.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>
#include <signal.h>

namespace DB::FailPoints
{
extern const char force_set_dtfile_exist_when_acquire_id[];
} // namespace DB::FailPoints

void fault_signal_handler(int signum)
{
    ::signal(signum, SIG_DFL);
    std::cerr << "Received signal " << strsignal(signum) << std::endl;
    std::cerr << StackTrace().toString() << std::endl;
    ::raise(signum);
}

void install_fault_signal_handlers(std::initializer_list<int> signums)
{
    for (auto signum : signums)
    {
        ::signal(signum, fault_signal_handler);
    }
}

class ThrowListener : public testing::EmptyTestEventListener
{
    void OnTestPartResult(const testing::TestPartResult & result) override
    {
        if (result.type() == testing::TestPartResult::kFatalFailure)
        {
            throw ::testing::AssertionException(result);
        }
    }
};


// TODO: Optmize set-up & tear-down process which may cost more than 2s. It's NOT friendly for gtest_parallel.
int main(int argc, char ** argv)
{
    install_fault_signal_handlers({SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGTERM});

    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();

#ifdef FIU_ENABLE
    fiu_init(0); // init failpoint

    DB::FailPointHelper::enableFailPoint(DB::FailPoints::force_set_dtfile_exist_when_acquire_id);
#endif

    ::testing::InitGoogleTest(&argc, argv);
    ::testing::UnitTest::GetInstance()->listeners().Append(new ThrowListener);

    auto ret = RUN_ALL_TESTS();

    DB::tests::TiFlashTestEnv::shutdown();

    return ret;
}
