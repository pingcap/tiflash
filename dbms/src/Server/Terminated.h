#include <atomic>

namespace DB::Terminated
{
inline std::atomic_bool server_terminated = false;
}