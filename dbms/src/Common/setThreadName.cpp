#if defined(__APPLE__)
#include <pthread.h>
#elif defined(__FreeBSD__)
#include <pthread.h>
#include <pthread_np.h>
#else
#include <sys/prctl.h>
#endif

#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <cstring>
#include <iostream>

namespace DB
{
namespace ErrorCodes
{
extern const int PTHREAD_ERROR;
}
} // namespace DB

void setThreadName(const char * tname)
{
    constexpr auto MAX_LEN = 15; // thread name will be tname[:MAX_LEN]
    if (std::strlen(tname) > MAX_LEN)
        std::cerr << "set thread name " << tname << " is too long and will be truncated by system\n";

#if defined(__FreeBSD__)
    pthread_set_name_np(pthread_self(), tname);
    return;

#elif defined(__APPLE__)
    if (0 != pthread_setname_np(tname))
#else
    if (0 != prctl(PR_SET_NAME, tname, 0, 0, 0))
#endif
    DB::throwFromErrno("Cannot set thread name " + std::string(tname), DB::ErrorCodes::PTHREAD_ERROR);
}

std::string getThreadName()
{
    constexpr auto MAX_LEN = 15;
    std::string name(MAX_LEN + 1, '\0'); // '\0' terminated

#if defined(__APPLE__)
    if (pthread_getname_np(pthread_self(), name.data(), name.size()))
        throw DB::Exception("Cannot get thread name with pthread_getname_np()", DB::ErrorCodes::PTHREAD_ERROR);
#elif defined(__FreeBSD__)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), name.data(), name.size()))
//        throw DB::Exception("Cannot get thread name with pthread_get_name_np()", DB::ErrorCodes::PTHREAD_ERROR);
#else
    if (0 != prctl(PR_GET_NAME, name.data(), 0, 0, 0))
        DB::throwFromErrno("Cannot get thread name with prctl(PR_GET_NAME)", DB::ErrorCodes::PTHREAD_ERROR);
#endif

    name.resize(std::strlen(name.data()));
    return name;
}
