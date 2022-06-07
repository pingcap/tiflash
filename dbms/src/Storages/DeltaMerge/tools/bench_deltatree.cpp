#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <benchmark/benchmark.h>

#include <mutex>
#include <thread>
using namespace DB;
using namespace DB::DM;

class BenchValueSpace;
using ArenaTree = DeltaTree<BenchValueSpace, DT_M, DT_F, DT_S, ArenaMemoryResource>;
using SystemTree = DeltaTree<BenchValueSpace, DT_M, DT_F, DT_S, SystemResource>;
using LevelTree = DeltaTree<BenchValueSpace, DT_M, DT_F, DT_S, MultiLevelResource>;

class BenchValueSpace
{
    using ValueSpace = BenchValueSpace;

public:
    void removeFromInsert(UInt64)
    {
    }

    void removeFromModify(UInt64, size_t)
    {
    }

    UInt64 withModify(UInt64 old_tuple_id, const ValueSpace & /*modify_value_space*/, const RefTuple &) // NOLINT(readability-convert-member-functions-to-static)
    {
        return old_tuple_id;
    }
};

template <class Tree>
static void BM_deltatree(benchmark::State & state)
{
    static Tree center;
    static std::mutex center_lock;
    static std::atomic_size_t finished;
    Tree tree;

    for (auto _ : state)
    {
        for (int j = 0; j < 64; ++j)
        {
            {
                std::unique_lock lock{center_lock};
                tree = center;
            }
            for (int i = 0; i < 8192; ++i)
            {
                tree.addInsert(i, i);
            }
            for (int i = 0; i < 8192; ++i)
            {
                tree.addDelete(i);
            }
            {
                std::unique_lock lock{center_lock};
                center.swap(tree);
            }
        }
        finished++;

        if (state.thread_index() == 0)
        {
            while (finished.load() != static_cast<size_t>(state.threads()))
            {
                std::this_thread::yield();
            }
            center = Tree{};
            finished = 0;
        } else {
            while (finished.load()) {
                std::this_thread::yield();
            }
        }
    }
}
BENCHMARK_TEMPLATE(BM_deltatree, LevelTree)->Threads(std::thread::hardware_concurrency())->Iterations(50);
BENCHMARK_TEMPLATE(BM_deltatree, SystemTree)->Threads(std::thread::hardware_concurrency())->Iterations(50);
BENCHMARK_TEMPLATE(BM_deltatree, ArenaTree)->Threads(std::thread::hardware_concurrency())->Iterations(50);
BENCHMARK_MAIN();