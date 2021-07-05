#include <Common/FailPoint.h>
#include <Storages/DeltaMerge/tests/stress/DeltaMergeStoreProxy.h>

#include <boost/program_options.hpp>

using StressOptions = DB::DM::tests::StressOptions;

StressOptions parseStressOptions(int argc, char * argv[])
{
    using boost::program_options::value;
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()("help", "produce help message")("mode", value<String>()->default_value(""), "stress mode: wo, ro, rw")(
        "insert_concurrency", value<UInt32>()->default_value(58), "number of insert thread")(
        "update_concurrency", value<UInt32>()->default_value(40), "number of update thread")(
        "delete_concurrency", value<UInt32>()->default_value(2), "number of delete thread")(
        "write_sleep_us", value<UInt32>()->default_value(20), "sleep microseconds between write operators")(
        "write_rows_per_block", value<UInt32>()->default_value(1), "number of rows per write(insert or update)")(
        "read_concurrency", value<UInt32>()->default_value(20), "number of read thread")(
        "read_sleep_us", value<UInt32>()->default_value(100), "sleep microseconds between read operations")(
        "gen_total_rows", value<UInt64>()->default_value(10000000), "generate data total rows")(
        "gen_rows_per_block", value<UInt32>()->default_value(64), "generate data rows per block")(
        "gen_concurrency", value<UInt32>()->default_value(100), "number of generate thread");

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") > 0)
    {
        std::cout << desc << std::endl;
        exit(-1);
    }

    StressOptions stress_options;
    stress_options.mode                 = options["mode"].as<String>();
    stress_options.insert_concurrency   = options["insert_concurrency"].as<UInt32>();
    stress_options.update_concurrency   = options["update_concurrency"].as<UInt32>();
    stress_options.delete_concurrency   = options["delete_concurrency"].as<UInt32>();
    stress_options.write_sleep_us       = options["write_sleep_us"].as<UInt32>();
    stress_options.write_rows_per_block = options["write_rows_per_block"].as<UInt32>();
    stress_options.read_concurrency     = options["read_concurrency"].as<UInt32>();
    stress_options.read_sleep_us        = options["read_sleep_us"].as<UInt32>();
    stress_options.gen_total_rows       = options["gen_total_rows"].as<UInt64>();
    stress_options.gen_rows_per_block   = options["gen_rows_per_block"].as<UInt32>();
    stress_options.gen_concurrency      = options["gen_concurrency"].as<UInt32>();

    std::cout << " mode: " << stress_options.mode << " insert_concurrency: " << stress_options.insert_concurrency
              << " update_concurrency: " << stress_options.update_concurrency
              << " delete_concurrency: " << stress_options.delete_concurrency << " write_sleep_us: " << stress_options.write_sleep_us
              << " write_row_per_block: " << stress_options.write_rows_per_block << " read_concurrency: " << stress_options.read_concurrency
              << " read_sleep_us: " << stress_options.read_sleep_us << " gen_row_count: " << stress_options.gen_total_rows
              << " gen_row_per_block: " << stress_options.gen_rows_per_block << " gen_concurrency: " << stress_options.gen_concurrency
              << std::endl;

    return stress_options;
}

void init()
{
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();
    fiu_init(0);
}

int main(int argc, char * argv[])
{
    init();
    auto opts = parseStressOptions(argc, argv);

    DB::DM::tests::DeltaMergeStoreProxy store_proxy(opts);

    try
    {
        store_proxy.genMultiThread();
        store_proxy.waitGenThreads();

        store_proxy.readMultiThread();
        store_proxy.insertMultiThread();
        store_proxy.updateMultiThread();
        store_proxy.deleteMultiThread();

        store_proxy.waitReadThreads();
        store_proxy.waitInsertThreads();
        store_proxy.waitUpdateThreads();
        store_proxy.waitDeleteThreads();
    }
    catch (std::exception & e)
    {
        LOG_INFO(&Poco::Logger::get("DeltaMergeStoreProxy"), e.what());
    }
    catch (...)
    {
        LOG_INFO(&Poco::Logger::get("DeltaMergeStoreProxy"), "Unknow exception");
    }
    DB::tests::TiFlashTestEnv::shutdown();
    return 0;
}
