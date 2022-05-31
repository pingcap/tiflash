#pragma once
#ifdef __linux__
#define TIFLASH_HAS_NUMACTL
#include <common/logger_useful.h>
#include <dlfcn.h>

#include <cstddef>
extern "C" {
struct bitmask;
}

namespace common::numa
{
#define TIFLASH_NUMA_FUNCTION_LIST_APPLY(M)                                                            \
    M(mbind, long (*)(void *, unsigned long, int, const unsigned long *, unsigned long, unsigned int)) \
    M(set_mempolicy, long (*)(int, const unsigned long *, unsigned long))                              \
    M(numa_parse_nodestring, struct bitmask * (*)(const char *))                                       \
    M(numa_parse_nodestring_all, struct bitmask * (*)(const char *))                                   \
    M(numa_set_interleave_mask, void (*)(struct bitmask *))                                            \
    M(numa_interleave_memory, void (*)(void *, size_t, struct bitmask *))                              \
    M(numa_set_preferred, void (*)(int))                                                               \
    M(numa_set_localalloc, void (*)(void))                                                             \
    M(numa_set_membind, void (*)(struct bitmask *))                                                    \
    M(numa_set_membind_balancing, void (*)(struct bitmask *))                                          \
    M(numa_bind, void (*)(struct bitmask *))                                                           \
    M(numa_available, int (*)(void))                                                                   \
    M(numa_max_possible_node, int (*)(void))                                                           \
    M(numa_num_possible_nodes, int (*)())                                                              \
    M(numa_max_node, int (*)(void))                                                                    \
    M(numa_num_configured_nodes, struct bitmask * (*)(void))                                           \
    M(numa_num_configured_cpus, int (*)(void))                                                         \
    M(numa_allocate_cpumask, struct bitmask * (*)(void))                                               \
    M(numa_allocate_nodemask, struct bitmask * (*)(void))                                              \
    M(numa_bitmask_free, void (*)(struct bitmask *))                                                   \
    M(numa_all_nodes_ptr, struct bitmask **)                                                           \
    M(numa_no_nodes_ptr, struct bitmask **)                                                            \
    M(numa_all_cpus_ptr, struct bitmask **)

class NumaCTL
{
    // define function types
#define TIFLASH_DECLARE_NUMA_FUNCTION_TYPE(NAME, ...) \
    using NAME##_func_t = __VA_ARGS__;
    TIFLASH_NUMA_FUNCTION_LIST_APPLY(TIFLASH_DECLARE_NUMA_FUNCTION_TYPE);
#undef TIFLASH_DECLARE_NUMA_FUNCTION_TYPE
    void * handle;

public:
    // define functions
#define TIFLASH_DEFINE_FUNCTION(NAME, ...) \
    const NAME##_func_t NAME;
    TIFLASH_NUMA_FUNCTION_LIST_APPLY(TIFLASH_DEFINE_FUNCTION)
#undef TIFLASH_DEFINE_FUNCTIONS

private:
    template <typename Type>
    static Type loadSymbol(void * handle, const char * symbol_name, Poco::Logger * log)
    {
        ::dlerror();
        auto res = reinterpret_cast<Type>(::dlsym(handle, symbol_name));
        if (nullptr != log)
        {
            if (nullptr == res)
            {
                const char * err = ::dlerror();
                LOG_FMT_ERROR(log, "failed to load {}: {}", symbol_name, err ? err : "unknown error");
            }
            else
            {
                LOG_FMT_INFO(log, "successfully loaded symbol: {}({})", symbol_name, reinterpret_cast<void *>(res));
            }
        }
        return res;
    }

    static void * openLibrary(const char * pattern, Poco::Logger * log)
    {
        const auto * resolved = pattern ? pattern : "libnuma.so.1";
        auto * res = ::dlopen(resolved, RTLD_LAZY | RTLD_LOCAL);
        if (nullptr != log)
        {
            auto err = ::dlerror();
            if (nullptr == res)
            {
                LOG_FMT_ERROR(log, "failed to load libnuma from {}: {}", resolved, err ? err : "unknown error");
            }
            else
            {
                LOG_FMT_ERROR(log, "loaded libnuma from {}({})", resolved, res);
            }
        }
        return res;
    }

#define TIFLASH_NUMA_INIT_FUNCTION(NAME, ...) , NAME(loadSymbol<NAME##_func_t>(handle, #NAME, log))
public:
    explicit NumaCTL(const char * pattern = nullptr, Poco::Logger * log = nullptr)
        : handle(openLibrary(pattern, log))
            TIFLASH_NUMA_FUNCTION_LIST_APPLY(TIFLASH_NUMA_INIT_FUNCTION)
    {}
#undef TIFLASH_NUMA_INIT_FUNCTION

    void free_nodemask(struct bitmask * mask)
    {
        if (!mask || mask == *numa_no_nodes_ptr)
            return;
        this->numa_bitmask_free(mask);
    }

    ~NumaCTL()
    {
        if (handle)
        {
            ::dlclose(handle);
        }
    }
};
#undef TIFLASH_NUMA_FUNCTION_LIST_APPLY
} // namespace common::numa

#endif