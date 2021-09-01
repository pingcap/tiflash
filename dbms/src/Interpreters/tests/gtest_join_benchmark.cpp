#include <DataStreams/NativeBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Join.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <chrono>
#include <fstream>

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

    using Blocks = std::vector<Block>;
    using Workers = std::vector<std::thread>;

    using TestData = std::vector<Blocks>;
    using ResultMap = std::unordered_map<Int64, size_t>;

    struct Result
    {
        size_t count = 0;
        ResultMap map;
    };

    TestData getTestData(const String & folder, const String & prefix, size_t n)
    {
        TestData data;
        data.resize(n);

        for (size_t i = 0; i < n; ++i)
        {
            Reader reader(fmt::format("{}/{}-{}.data", folder, prefix, i));

            while (true)
            {
                auto block = reader.read();

                if (block)
                    data[i].emplace_back(std::move(block));
                else
                    break;
            }
        }

        return data;
    }

    Result getAnswer(const String & folder)
    {
        Result answer;

        std::ifstream file(fmt::format("{}/result.csv", folder));

        Int64 key;
        size_t count;
        while (file >> key >> count)
        {
            answer.count += count;
            answer.map[key] += count;
        }

        return answer;
    }
};

TEST_F(JoinBenchmark, SmallParallelJoin)
{
    // TPC-H sf=1
    // select l_partkey + c_nationkey as result, count(*) from lineitem, customer where l_partkey = c_nationkey group by result order by result

    constexpr size_t n_repeat = 10;
    constexpr bool only_check_total_rows = false;

    // test data are outside tics repo.
    String data_folder = "/pingcap/data/small-parallel-join";
    auto build_data = getTestData(data_folder, "build", 3);
    auto original_probe_data = getTestData(data_folder, "probe", 4);
    auto answer = getAnswer(data_folder);

    for (size_t round = 0; round < n_repeat; ++round)
    {
        // join.joinBlock is done in-place, so we have to clone test data blocks first.
        // note: due to COW, probe_data and original_probe_data share the same underlying memories at here,
        // but after join.joinBlock, they will not share memories any more.
        auto probe_data = original_probe_data;

        using clock = std::chrono::steady_clock;
        auto begin_time = clock::now();

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

        join.setSampleBlock(build_data[0][0].cloneEmpty());

        Workers build_workers;
        build_workers.reserve(3);

        for (size_t i = 0; i < 3; ++i)
        {
            build_workers.emplace_back([&, i] {
                for (const auto & block : build_data[i])
                    join.insertFromBlock(block, i);
            });
        }

        for (size_t i = 0; i < 3; ++i)
        {
            build_workers[i].join();
        }

        join.setFinishBuildTable(true);

        Workers probe_workers;
        probe_workers.reserve(4);

        for (size_t i = 0; i < 4; ++i)
        {
            probe_workers.emplace_back([&, i] {
                for (auto & block : probe_data[i])
                    join.joinBlock(block);
            });
        }

        for (size_t i = 0; i < 4; ++i)
        {
            probe_workers[i].join();
        }

        auto end_time = clock::now();
        auto time_used_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time).count();
        LOG_INFO(&Poco::Logger::get("SmallParallelJoin"), fmt::format("round {}: {}ms", round, time_used_in_ms));

        // check result.

        size_t count = 0;
        for (size_t i = 0; i < 4; ++i)
        {
            for (const Block & block : probe_data[i])
                count += block.rows();
        }

        ASSERT_EQ(answer.count, count);

        if constexpr (!only_check_total_rows)
        {
            // std::cout << probe_data[0][10].dumpStructure() << std::endl;
            String l_partkey_name = "__QB_7_l_partkey";
            String c_nationkey_name = "__QB_6_exchange_receiver_0";

            std::unordered_map<Int64, size_t> map;
            for (size_t i = 0; i < 4; ++i)
            {
                for (const auto & block : probe_data[i])
                {
                    auto l_partkey = block.getByName(l_partkey_name).column;
                    auto c_nationkey = block.getByName(c_nationkey_name).column;

                    for (size_t j = 0; j < block.rows(); ++j)
                    {
                        Int64 key = l_partkey->getInt(j) + c_nationkey->getInt(j);
                        ++map[key];
                    }
                }
            }

            ASSERT_EQ(answer.map, map);
        }
    }
}

} // namespace tests

} // namespace DB
