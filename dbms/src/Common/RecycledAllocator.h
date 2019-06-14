#pragma once

#include <Common/ArenaWithFreeLists.h>

namespace DB
{
/// A simple wrap around ArenaWithFreeLists.
class RecycledAllocator : public boost::noncopyable
{
public:
    RecycledAllocator(const size_t initial_size = 65536, const size_t growth_factor = 2) : arena(initial_size, growth_factor) {}

    char * alloc(size_t size) { return arena.alloc(size); }

    void free(char * buf, const size_t size) { arena.free(buf, size); }

    std::shared_ptr<char> createMemoryReference(char * addr, size_t size)
    {
        return std::shared_ptr<char>(addr, [=](char * a) { arena.free(a, size); });
    }

private:
    ArenaWithFreeLists arena;
};

} // namespace DB