#include <Common/MemoryTracker.h>
#include <Common/NumaAwareMemoryHierarchy.h>
#include <common/defines.h>
#include <common/numa.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>

#if defined(__linux__)
#if !defined(MAP_HUGETLB)
#define MAP_HUGETLB 0x40000
#endif
#if !defined(MAP_HUGE_SHIFT)
#define MAP_HUGE_SHIFT 26
#endif
#if !defined(MAP_HUGE_2MB)
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif
#if !defined(MADV_FREE)
#define MADV_FREE 8
#endif
#endif

namespace DB::NumaAwareMemoryHierarchy
{

const size_t PAGE_SIZE = ::sysconf(_SC_PAGESIZE);

static inline void * allocateImpl(size_t size)
{
    static std::atomic_bool hugetlb_allowed{true};

    void * result = MAP_FAILED;
    const int prot_flags = PROT_READ | PROT_WRITE;
    int mmap_flags = MAP_ANONYMOUS | MAP_PRIVATE;

    auto system_allocate = [&](int flags) {
        result = ::mmap(nullptr, size, prot_flags, flags, -1, 0);
    };

    if (size % SIZE_2MIB == 0 && hugetlb_allowed.load(std::memory_order_relaxed))
    {
#if defined(__linux__)
        if (MAP_FAILED == result)
        {
            system_allocate(mmap_flags | MAP_HUGETLB | MAP_HUGE_2MB);
        }
        if (MAP_FAILED == result)
        {
            hugetlb_allowed.store(false, std::memory_order_relaxed);
        }
#endif
    }

    if (MAP_FAILED == result)
    {
        system_allocate(mmap_flags);
    }

    return result;
}

static inline void * allocateOSPages(size_t count)
{
    // allocate result
    auto * result = allocateImpl(count * PAGE_SIZE);
    auto address = reinterpret_cast<uintptr_t>(result);
    // check alignment
    if (unlikely(address % PAGE_SIZE != 0))
    {
        ::munmap(result, count * PAGE_SIZE);
        // over-allocate to enforce alignment
        result = allocateImpl(count * (PAGE_SIZE + 1));
        address = reinterpret_cast<uintptr_t>(result);
        auto offset = PAGE_SIZE - address % PAGE_SIZE;
        auto * begin = static_cast<char *>(result);
        // truncate extra area
        ::munmap(begin, offset);
        ::munmap(begin + count * PAGE_SIZE + offset, PAGE_SIZE - offset);
        result = begin + offset;
    }
    CurrentMemoryTracker::alloc(count * PAGE_SIZE);
    return result;
}

void decommitPages(void * pointer, size_t count)
{
    int result = -1;
#if defined(MADV_FREE)
    result = ::madvise(pointer, count * PAGE_SIZE, MADV_FREE);
#endif
    if (0 != result)
    {
        ::madvise(pointer, count * PAGE_SIZE, MADV_DONTNEED);
    }
    CurrentMemoryTracker::free(count * PAGE_SIZE);
}

GlobalPagePool::GlobalPagePool()
    : numa_count(common::numa::getNumaCount())
    , numa_freelists(
          static_cast<PerNumaFreeList *>(
              ::operator new (sizeof(PerNumaFreeList) * numa_count, std::align_val_t{alignof(PerNumaFreeList)})))
{
    for (size_t i = 0; i < numa_count; ++i)
    {
        ::new (std::addressof(numa_freelists[i])) PerNumaFreeList{i};
    }
}

GlobalPagePool::~GlobalPagePool()
{
    std::destroy_n(numa_freelists, numa_count);
    ::operator delete (numa_freelists, sizeof(PerNumaFreeList) * numa_count, std::align_val_t{alignof(PerNumaFreeList)});
}

GlobalPagePool::PerNumaFreeList & GlobalPagePool::getFreeList() const
{
    auto numa = common::numa::getNumaNode() % numa_count;
    return numa_freelists[numa];
}

static inline void * allocateFrom(std::mutex & lock, Node *& freelist, size_t numa)
{
    Node * result = nullptr;

    {
        std::unique_lock hold{lock};
        result = freelist;
        UNUSED(hold);
        if (result)
        {
            freelist = result->next;
            return result;
        }
    }

    {
        result = static_cast<Node *>(allocateOSPages(SIZE_2MIB / PAGE_SIZE));
        common::numa::bindMemoryToNuma(result, SIZE_2MIB, numa);
    }

    return result;
}

static inline void recycleTo(std::mutex & lock, Node *& freelist, void * p)
{
    decommitPages(p, SIZE_2MIB / PAGE_SIZE);
    {
        std::unique_lock hold{lock};
        freelist = ::new (p) Node{freelist};
        UNUSED(hold);
    }
}

void * GlobalPagePool::PerNumaFreeList::allocate()
{
    return allocateFrom(lock_2mb, freelist_2mb, numa);
}

void GlobalPagePool::PerNumaFreeList::recycle(void * p)
{
    recycleTo(lock_2mb, freelist_2mb, p);
}

GlobalPagePool::PerNumaFreeList::~PerNumaFreeList()
{
    while (Node * n = freelist_2mb)
    {
        freelist_2mb = n->next;
        ::munmap(n, SIZE_2MIB);
    }
}
} // namespace DB::NumaAwareMemoryHierarchy