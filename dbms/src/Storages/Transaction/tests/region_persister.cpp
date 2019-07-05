#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <ext/scope_guard.h>

#include "region_helper.h"

using namespace DB;

const std::string dir_path = "./region_persister_tmp_/";


int main(int, char **)
{
    SCOPE_EXIT({
        // remove tmp dir
        Poco::File(dir_path).remove(true);
    });
    Poco::File dir(dir_path);
    if (dir.exists())
        dir.remove(true);
    dir.createDirectory();

    bool suc = true;
    {
        auto peer = createPeer(100, true);
        auto path = dir_path + "peer.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        auto size = writeBinary2(peer, write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_peer = readPeer(read_buf);
        ASSERT_CHECK_EQUAL(new_peer, peer, suc);
    }

    {
        auto region_info = createRegionInfo(233, "", "");
        auto path = dir_path + "region_info.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        auto size = writeBinary2(region_info, write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_region_info = readRegion(read_buf);
        ASSERT_CHECK_EQUAL(new_region_info, region_info, suc);
    }

    {
        RegionMeta meta = createRegionMeta(888);
        auto path = dir_path + "meta.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        auto size = meta.serialize(write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_meta = RegionMeta::deserialize(read_buf);
        ASSERT_CHECK_EQUAL(new_meta, meta, suc);
    }

    {
        auto region = std::make_shared<Region>(createRegionMeta(100));
        TiKVKey key = RecordKVFormat::genKey(100, 323, 9983);
        region->insert("default", key, TiKVValue("value1"));
        region->insert("write", key, RecordKVFormat::encodeWriteCfValue('P', 0));
        region->insert("lock", key, RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

        auto path = dir_path + "region.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = region->serialize(write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(region_ser_size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_region = Region::deserialize(read_buf);


        ASSERT_CHECK_EQUAL(*new_region, *region, suc);
    }

    {
        RegionPtr region = nullptr;
        {
            metapb::Peer peer;
            raft_serverpb::RegionLocalState region_state;
            raft_serverpb::RaftApplyState apply_state;

            peer.set_id(6666);
            peer.set_is_learner(true);
            peer.set_store_id(6667);

            {
                metapb::Region region_;
                region_.set_id(6668);
                *region_.mutable_start_key() = "6669";
                *region_.mutable_end_key() = "6670";
                region_.mutable_region_epoch()->set_conf_ver(6671);
                region_.mutable_region_epoch()->set_version(6672);
                *region_.add_peers() = peer;

                *region_state.mutable_region() = region_;
                region_state.set_state(raft_serverpb::PeerState::Merging);
                region_state.mutable_merge_state()->set_min_index(6674);
                region_state.mutable_merge_state()->set_commit(6675);

                {
                    metapb::Region tmp;
                    tmp.set_id(6676);
                    *tmp.mutable_start_key() = "6677";
                    *tmp.mutable_end_key() = "6678";

                    *region_state.mutable_merge_state()->mutable_target() = std::move(tmp);
                }
            }

            apply_state.set_applied_index(6671);
            apply_state.mutable_truncated_state()->set_index(6672);
            apply_state.mutable_truncated_state()->set_term(6673);

            RegionMeta meta(std::move(peer), std::move(apply_state), 6679, std::move(region_state));
            region = std::make_shared<Region>(std::move(meta));
        }

        TiKVKey key = RecordKVFormat::genKey(100, 323, 9983);
        region->insert("default", key, TiKVValue("value1"));
        region->insert("write", key, RecordKVFormat::encodeWriteCfValue('P', 0));
        region->insert("lock", key, RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

        auto path = dir_path + "region_state.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = region->serialize(write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(region_ser_size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_region = Region::deserialize(read_buf);

        ASSERT_CHECK_EQUAL(*new_region, *region, suc);
    }

    auto remove_dir = [](const std::string & path) {
        Poco::File file(path);
        if (file.exists())
            file.remove(true);
    };

    {
        std::string path = dir_path + "broken_file";
        remove_dir(path);
        SCOPE_EXIT({ remove_dir(path); });

        size_t region_num = 100;
        RegionMap regions;
        RegionMap new_regions;
        {
            UInt64 diff = 0;
            PageStorage::Config config;
            config.file_roll_size = 128 * MB;
            RegionPersister persister(path);
            for (size_t i = 0; i < region_num; ++i)
            {
                auto region = std::make_shared<Region>(createRegionMeta(i));
                TiKVKey key = RecordKVFormat::genKey(100, i, diff++);
                region->insert("default", key, TiKVValue("value1"));
                region->insert("write", key, RecordKVFormat::encodeWriteCfValue('P', 0));
                region->insert("lock", key, RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

                persister.persist(region);

                regions.emplace(region->id(), region);
            }

            // If we truncate page data file, exception will throw instead of droping last region.
            auto meta_path = path + "/page_1_0/meta"; // First page
            Poco::File meta_file(meta_path);
            size_t size = meta_file.getSize();
            int rt = ::truncate(meta_path.c_str(), size - 1); // Remove last one byte
            ASSERT_CHECK(!rt, suc);
        }

        {
            RegionPersister persister(path);
            persister.restore(new_regions);
            for (size_t i = 0; i < region_num; ++i)
            {
                if (i == region_num - 1)
                {
                    // The last region is broken and should not exist.
                    ASSERT_CHECK_EQUAL(new_regions.find(i), new_regions.end(), suc);
                }
                else
                {
                    auto old_region = regions[i];
                    auto new_region = new_regions[i];
                    ASSERT_CHECK_EQUAL(*new_region, *old_region, suc);
                }
            }
        }
    }


    UInt64 diff = 0;
    auto test_func1 = [&](const std::string & path, const PageStorage::Config & config, int region_num, bool is_gc, bool clean_up) {
        if (clean_up)
            remove_dir(path);

        RegionPersister persister(path, config);
        RegionMap regions;
        for (int i = 0; i < region_num; ++i)
        {
            auto region = std::make_shared<Region>(createRegionMeta(i));
            TiKVKey key = RecordKVFormat::genKey(100, i, diff++);
            region->insert("default", key, TiKVValue("value1"));
            region->insert("write", key, RecordKVFormat::encodeWriteCfValue('P', 0));
            region->insert("lock", key, RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

            persister.persist(region);

            regions.emplace(region->id(), region);
        }

        if (is_gc)
            persister.gc();

        RegionMap new_regions;
        persister.restore(new_regions);

        for (int i = 0; i < region_num; ++i)
        {
            auto old_region = regions[i];
            auto new_region = new_regions[i];
            ASSERT_CHECK_EQUAL(*new_region, *old_region, suc);
        }

        if (clean_up)
            remove_dir(path);
    };

    auto run_test = [&](const std::string & path, bool sync_on_write) {
        Timepoint t1 = Clock::now();

        remove_dir(path);
        SCOPE_EXIT({ remove_dir(path); });

        {
            PageStorage::Config conf;
            conf.sync_on_write = sync_on_write;
            conf.file_roll_size = 1;
            conf.merge_hint_low_used_file_total_size = 1;

            test_func1(path, conf, 10, false, false);
            test_func1(path, conf, 10, true, false);

            test_func1(path, conf, 10, false, true);
            test_func1(path, conf, 10, true, true);
        }
        {
            PageStorage::Config conf;
            conf.sync_on_write = sync_on_write;
            conf.file_roll_size = 500;
            conf.merge_hint_low_used_file_total_size = 1;

            test_func1(path, conf, 100, false, false);
            test_func1(path, conf, 100, false, false);
            test_func1(path, conf, 100, false, false);
            test_func1(path, conf, 100, false, true);

            test_func1(path, conf, 100, true, false);
            test_func1(path, conf, 100, true, false);
            test_func1(path, conf, 100, true, false);
            test_func1(path, conf, 100, true, false);
        }
        {
            PageStorage::Config conf;
            conf.sync_on_write = sync_on_write;
            conf.file_roll_size = 500;
            conf.merge_hint_low_used_file_total_size = 1;

            test_func1(path, conf, 100, false, false);
            test_func1(path, conf, 100, false, false);
            test_func1(path, conf, 100, false, false);
            test_func1(path, conf, 100, false, true);

            test_func1(path, conf, 100, true, false);
            test_func1(path, conf, 100, true, false);
            test_func1(path, conf, 100, true, false);
            test_func1(path, conf, 100, true, false);
        }
        {
            PageStorage::Config conf;
            conf.sync_on_write = sync_on_write;

            test_func1(path, conf, 10000, false, false);
            test_func1(path, conf, 10000, false, false);
            test_func1(path, conf, 10000, false, false);
            test_func1(path, conf, 10000, false, false);
            test_func1(path, conf, 10000, false, false);
            test_func1(path, conf, 10000, false, false);
            test_func1(path, conf, 10000, false, false);
        }
        {
            PageStorage::Config conf;
            conf.sync_on_write = sync_on_write;

            test_func1(path, conf, 10000, true, false);
            test_func1(path, conf, 10000, true, false);
            test_func1(path, conf, 10000, true, false);
            test_func1(path, conf, 10000, true, false);
            test_func1(path, conf, 10000, true, false);
            test_func1(path, conf, 10000, true, false);
            test_func1(path, conf, 10000, true, false);
        }

        Timepoint t2 = Clock::now();
        Seconds seconds = std::chrono::duration_cast<Seconds>(t2 - t1);
        std::cout << "sync_on_write[" << sync_on_write << "] time: " << seconds.count() << " seconds" << std::endl;
    };

    run_test(dir_path + "region_persist_storage_sow_false", false);
    run_test(dir_path + "region_persist_storage_sow_true", true);


    return suc ? 0 : 1;
}
