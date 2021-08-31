#include <DataStreams/NativeBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Join.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class JoinBenchmark : public ::testing::Test
{
protected:
    class Reader
    {
    public:
        Reader(const String & path)
            : buffer(path)
            , stream(buffer, 0)
        {
            stream.readPrefix();
        }

        ~Reader()
        {
            stream.readSuffix();
            buffer.close();
        }

        Block read()
        {
            return stream.read();
        }

    private:
        ReadBufferFromFile buffer;
        NativeBlockInputStream stream;
    };
};

TEST_F(JoinBenchmark, ParallelJoin)
{
    // * thread #153, name = 'TiFlashMain', stop reason = breakpoint 1.1
    //     frame #0: 0x000000001763cfd5 tiflash`DB::DAGQueryBlockInterpreter::executeJoin(this=0x00007ff7ec8fb8e8, join=0x00007ff866a67600, pipeline=0x00007ff7ec8fb840, right_query=0x00007ff7ec8fb690) at DAGQueryBlockInterpreter.cpp:522:23
    //    519 	    size_t max_block_size_for_cross_join = settings.max_block_size;
    //    520 	    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size_for_cross_join = 1; });
    //    521
    // -> 522 	    JoinPtr joinPtr = std::make_shared<Join>(left_key_names,
    //    523 	        right_key_names,
    //    524 	        true,
    //    525 	        SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
    // (lldb) p left_key_names
    // (DB::Names) $0 = size=1 {
    //   [0] = "__QB_7_l_partkey"
    // }
    // (lldb) p right_key_names
    // (DB::Names) $1 = size=1 {
    //   [0] = "_r_k___QB_6_exchange_receiver_0"
    // }
    // (lldb) p settings.max_rows_in_join
    // (const DB::SettingUInt64) $2 = (value = 0, changed = false)
    // (lldb) p settings.max_bytes_in_join
    // (const DB::SettingUInt64) $3 = (value = 0, changed = false)
    // (lldb) p settings.join_overflow_mode
    // (const DB::SettingOverflowMode<false>) $4 = (value = THROW, changed = false)
    // (lldb) p kind
    // (DB::ASTTableJoin::Kind) $5 = Inner
    // (lldb) p strictness
    // (DB::ASTTableJoin::Strictness) $6 = All
    // (lldb) p join_build_concurrency
    // (size_t) $7 = 4
    // (lldb) p collators
    // (TiDB::TiDBCollators) $8 = size=1 {
    //   [0] = nullptr {
    //     __ptr_ = nullptr
    //   }
    // }
    // (lldb) p left_filter_column_name
    // (DB::String) $9 = ""
    // (lldb) p right_filter_column_name
    // (DB::String) $10 = ""
    // (lldb) p other_filter_column_name
    // (DB::String) $11 = ""
    // (lldb) p other_eq_filter_from_in_column_name
    // (DB::String) $12 = ""
    // (lldb) p other_condition_expr
    // (DB::ExpressionActionsPtr) $13 = nullptr {
    //   __ptr_ = nullptr
    // }
    // (lldb) p max_block_size_for_cross_join
    // (size_t) $14 = 65536

    Join join(
        {"__QB_7_l_partkey"},
        {"_r_k___QB_6_exchange_receiver_0"},
        true,
        SizeLimits(0, 0, OverflowMode::THROW),
        ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness::All,
        4,
        {nullptr},
        "",
        "",
        "",
        "",
        nullptr,
        65536);

    String data_folder = "/pingcap/data/join-1";

    {
        Reader reader(data_folder + "/build-0.data");
        join.setSampleBlock(reader.read().cloneEmpty());
    }

    // TODO:
    // 1. read all blocks in advance.
    // 2. verify result.

    std::vector<std::thread> build_workers;
    build_workers.reserve(3);

    for (int i = 0; i < 3; ++i)
    {
        build_workers.emplace_back([&, i] {
            Reader reader(fmt::format("{}/build-{}.data", data_folder, i));

            Block block;
            do
            {
                block = reader.read();
                join.insertFromBlock(block, i);
            } while (block);
        });
    }

    for (int i = 0; i < 3; ++i)
    {
        build_workers[i].join();
    }

    join.setFinishBuildTable(true);

    std::vector<std::thread> probe_workers;
    probe_workers.reserve(4);

    for (int i = 0; i < 4; ++i)
    {
        probe_workers.emplace_back([&, i] {
            Reader reader(fmt::format("{}/probe-{}.data", data_folder, i));

            Block block;
            do
            {
                block = reader.read();
                join.joinBlock(block);
            } while (block);
        });
    }

    for (int i = 0; i < 4; ++i)
    {
        probe_workers[i].join();
    }
}

} // namespace tests

} // namespace DB
