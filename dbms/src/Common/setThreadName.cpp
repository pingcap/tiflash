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
    DB::throwFromErrno("Cannot set thread name " + std::string(tname));
}
