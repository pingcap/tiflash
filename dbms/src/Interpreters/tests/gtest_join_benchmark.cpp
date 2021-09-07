#include <DataStreams/NativeBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Join.h>
#include <Poco/Glob.h>
#include <Poco/JSON/Parser.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <chrono>
#include <fstream>

namespace DB
{
namespace tests
{
class JoinBenchmark : public ::testing::TestWithParam<const char *>
{
public:
    Poco::Logger * logger = &Poco::Logger::get(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name());

    using BlockStream = std::vector<Block>;
    using BlockStreams = std::vector<BlockStream>;
    using Workers = std::vector<std::thread>;
    using JSONObjectPtr = Poco::JSON::Object::Ptr;
    using JSONArrayPtr = Poco::JSON::Array::Ptr;
    using JoinPtr = std::shared_ptr<Join>;

    static size_t getNumRows(const BlockStreams & streams)
    {
        size_t count = 0;
        for (const auto & stream : streams)
        {
            for (const auto & block : stream)
                count += block.rows();
        }

        return count;
    }

    static Block getSampleBlock(const BlockStreams & streams)
    {
        for (const auto & stream : streams)
        {
            if (!stream.empty())
                return stream.front().cloneEmpty();
        }

        throw TiFlashTestException("Can't provide sample block because all block streams are empty");
    }

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

    class TestDataSet
    {
    public:
        TestDataSet(String folder_)
            : folder(folder_)
        {
            config = readConfig(fmt::format("{}/config.json", folder));

            auto build_file_prefix = config->getValue<String>("build_file_prefix");
            auto probe_file_prefix = config->getValue<String>("probe_file_prefix");

            std::set<String> build_files, probe_files;
            Poco::Glob::glob(fmt::format("{}/{}-*.data", folder, build_file_prefix), build_files);
            Poco::Glob::glob(fmt::format("{}/{}-*.data", folder, probe_file_prefix), probe_files);

            num_build_threads = build_files.size();
            num_probe_threads = probe_files.size();

            build_streams = readBlockStreams(build_files);
            probe_streams = readBlockStreams(probe_files);
        }

        size_t getNumBuildThreads() const
        {
            return num_build_threads;
        }

        size_t getNumProbeThreads() const
        {
            return num_probe_threads;
        }

        Block getBuildSampleBlock() const
        {
            return getSampleBlock(build_streams);
        }

        Block getProbeSampleBlock() const
        {
            return getSampleBlock(probe_streams);
        }

        const BlockStream & getBuildStream(size_t i) const
        {
            if (i >= num_build_threads)
                throw TiFlashTestException(fmt::format("Index is too large for build data: i = {}", i));

            return build_streams[i];
        }

        const BlockStream & getProbeStream(size_t i) const
        {
            if (i >= num_probe_threads)
                throw TiFlashTestException(fmt::format("Index is too large for probe data: i = {}", i));

            return probe_streams[i];
        }

        BlockStreams cloneProbeStreams() const
        {
            return probe_streams;
        }

        size_t getNumBuildRows() const
        {
            return getNumRows(build_streams);
        }

        size_t getNumProbeRows() const
        {
            return getNumRows(probe_streams);
        }

        size_t getNumResultRows() const
        {
            return config->getValue<size_t>("num_result_rows");
        }

        size_t getNumRepeat() const
        {
            return config->getValue<size_t>("num_repeat");
        }

        JoinPtr createJoin() const
        {
            auto limits = config->getObject("limits");

            // TODO: support for other & other_eq filters.
            return std::make_shared<Join>(
                toStrings(config->getArray("left_keys")),
                toStrings(config->getArray("right_keys")),
                true,
                SizeLimits(
                    limits->getValue<UInt64>("max_rows"),
                    limits->getValue<UInt64>("max_bytes"),
                    static_cast<OverflowMode>(limits->getValue<Int64>("overflow_mode"))),
                static_cast<ASTTableJoin::Kind>(config->getValue<Int64>("kind")),
                static_cast<ASTTableJoin::Strictness>(config->getValue<Int64>("strictness")),
                config->getValue<size_t>("join_build_concurrency"),
                TiDB::TiDBCollators{nullptr},
                config->getValue<String>("left_filter_column"),
                config->getValue<String>("right_filter_column"),
                config->getValue<String>("other_filter_column"),
                config->getValue<String>("other_eq_filter_column"),
                nullptr,
                config->getValue<size_t>("max_block_size"));
        }

    private:
        BlockStream readBlockStream(const String & path) const
        {
            Reader reader(path);

            BlockStream stream;
            while (true)
            {
                auto block = reader.read();

                if (block)
                    stream.emplace_back(std::move(block));
                else
                    break;
            }

            return stream;
        }

        BlockStreams readBlockStreams(const std::set<String> & paths) const
        {
            BlockStreams streams;
            streams.resize(paths.size());

            size_t i = 0;
            for (const auto & path : paths)
            {
                streams[i] = readBlockStream(path);
                ++i;
            }

            return streams;
        }

        JSONObjectPtr readConfig(const String & path) const
        {
            std::ifstream file(path);
            Poco::JSON::Parser parser;
            auto config = parser.parse(file).extract<JSONObjectPtr>();
            if (!config)
                throw TiFlashTestException(fmt::format("Failed to load config file: {}", path));

            return config;
        }

        Strings toStrings(const JSONArrayPtr & array) const
        {
            Strings vec;
            vec.reserve(array->size());
            for (auto it = array->begin(); it != array->end(); ++it)
            {
                vec.emplace_back(it->toString());
            }

            return vec;
        }

        String folder;
        size_t num_build_threads, num_probe_threads;
        BlockStreams build_streams, probe_streams;
        JSONObjectPtr config;
    };
};

TEST_P(JoinBenchmark, Run)
{
    // test data are outside tics repo.
    String data_folder = GetParam();
    TestDataSet dataset(data_folder);

    size_t num_repeat = dataset.getNumRepeat();
    size_t num_build_threads = dataset.getNumBuildThreads();
    size_t num_probe_threads = dataset.getNumProbeThreads();
    LOG_DEBUG(
        logger,
        fmt::format(
            "{}: "
            "num_repeat = {}, "
            "build = [threads = {}, rows = {}, columns = {}], "
            "probe = [threads = {}, rows = {}, columns = {}]",
            data_folder,
            num_repeat,
            num_build_threads,
            dataset.getNumBuildRows(),
            dataset.getBuildSampleBlock().columns(),
            num_probe_threads,
            dataset.getNumProbeRows(),
            dataset.getProbeSampleBlock().columns()));

    for (size_t round = 0; round < num_repeat; ++round)
    {
        // join.joinBlock is done in-place, so we have to clone test data blocks first.
        auto probe_streams = dataset.cloneProbeStreams();

        using clock = std::chrono::steady_clock;
        auto begin_time = clock::now();

        auto join = dataset.createJoin();
        join->setSampleBlock(dataset.getBuildSampleBlock());

        Workers build_workers;
        build_workers.reserve(num_build_threads);

        for (size_t i = 0; i < num_build_threads; ++i)
        {
            build_workers.emplace_back([&, i] {
                for (const auto & block : dataset.getBuildStream(i))
                    join->insertFromBlock(block, i);
            });
        }

        for (size_t i = 0; i < num_build_threads; ++i)
            build_workers[i].join();

        join->setFinishBuildTable(true);

        Workers probe_workers;
        probe_workers.reserve(num_probe_threads);

        for (size_t i = 0; i < num_probe_threads; ++i)
        {
            probe_workers.emplace_back([&, i] {
                for (auto & block : probe_streams[i])
                    join->joinBlock(block);
            });
        }

        for (size_t i = 0; i < num_probe_threads; ++i)
            probe_workers[i].join();

        auto end_time = clock::now();
        auto time_used_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time).count();
        LOG_INFO(logger, fmt::format("round {}: {}ms", round, time_used_in_ms));

        // check result.

        size_t expected = dataset.getNumResultRows();
        size_t actual = getNumRows(probe_streams);
        ASSERT_EQ(expected, actual);
    }
}

TEST_P(JoinBenchmark, BuildOnly)
{
    String data_folder = GetParam();
    TestDataSet dataset(data_folder);
    size_t num_repeat = dataset.getNumRepeat();
    size_t num_build_threads = dataset.getNumBuildThreads();
    LOG_DEBUG(
        logger,
        fmt::format(
            "{}: "
            "num_repeat = {}, "
            "build = [threads = {}, rows = {}, columns = {}]",
            data_folder,
            num_repeat,
            num_build_threads,
            dataset.getNumBuildRows(),
            dataset.getBuildSampleBlock().columns()));

    for (size_t round = 0; round < num_repeat; ++round)
    {
        using clock = std::chrono::steady_clock;
        auto begin_time = clock::now();

        auto join = dataset.createJoin();
        join->setSampleBlock(dataset.getBuildSampleBlock());

        Workers build_workers;
        build_workers.reserve(num_build_threads);

        for (size_t i = 0; i < num_build_threads; ++i)
        {
            build_workers.emplace_back([&, i] {
                for (const auto & block : dataset.getBuildStream(i))
                    join->insertFromBlock(block, i);
            });
        }

        for (size_t i = 0; i < num_build_threads; ++i)
            build_workers[i].join();

        join->setFinishBuildTable(true);

        auto end_time = clock::now();
        auto time_used_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time).count();
        LOG_INFO(
            logger,
            fmt::format(
                "round {}: {}ms, total_bytes = {}, total_rows = {}",
                round,
                time_used_in_ms,
                join->getTotalByteCount(),
                join->getTotalRowCount()));

        ASSERT_EQ(dataset.getNumBuildRows(), join->getTotalRowCount());
    }
}

INSTANTIATE_TEST_CASE_P(
    Simple,
    JoinBenchmark,
    ::testing::Values("/pingcap/data/small-inner-join-1", "/pingcap/data/large-inner-join-1"));

INSTANTIATE_TEST_CASE_P(
    SF20_Q9,
    JoinBenchmark,
    ::testing::Values(
        "/pingcap/data/SF20-Q9-HashJoin_50",
        "/pingcap/data/SF20-Q9-HashJoin_51",
        "/pingcap/data/SF20-Q9-HashJoin_52",
        "/pingcap/data/SF20-Q9-HashJoin_53",
        "/pingcap/data/SF20-Q9-HashJoin_180"));

} // namespace tests

} // namespace DB
