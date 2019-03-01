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
        auto                peer = createPeer(100, true);
        auto                path = dir_path + "peer.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        auto                size = writeBinary2(peer, write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto               new_peer = readPeer(read_buf);
        ASSERT_CHECK_EQUAL(new_peer, peer, suc);
    }

    {
        auto                region_info = createRegionInfo(233, "", "");
        auto                path        = dir_path + "region_info.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        auto                size = writeBinary2(region_info, write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto               new_region_info = readRegion(read_buf);
        ASSERT_CHECK_EQUAL(new_region_info, region_info, suc);
    }

    {
        RegionMeta          meta = createRegionMeta(888);
        auto                path = dir_path + "meta.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        auto                size = meta.serialize(write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto               new_meta = RegionMeta::deserialize(read_buf);
        ASSERT_CHECK_EQUAL(new_meta, meta, suc);
    }

    {
        auto region = std::make_shared<Region>(createRegionMeta(100));
        TiKVKey key = RecordKVFormat::genKey(100, 323, 9983);
        region->insert("default", key, TiKVValue("value1"));
        region->insert("write", key, TiKVValue("value1"));
        region->insert("lock", key, TiKVValue("value1"));

        auto                path = dir_path + "region.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = region->serialize(write_buf);
        write_buf.next();

        ASSERT_CHECK_EQUAL(region_ser_size, (size_t)Poco::File(path).getSize(), suc);
        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto               new_region = Region::deserialize(read_buf);


        ASSERT_CHECK_EQUAL(*new_region, *region, suc);
    }

    auto region1 = std::make_shared<Region>(createRegionMeta(1));
    auto region2 = std::make_shared<Region>(createRegionMeta(2));
    auto region3 = std::make_shared<Region>(createRegionMeta(3));

    size_t region_ser_size;
    {
        auto                path = dir_path + "region.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        region_ser_size = region1->serialize(write_buf);
        write_buf.next();
    }

    {
        RegionPersister persister(dir_path);
        persister.persist(region1);
        persister.persist(region1);
        persister.persist(region2);
        persister.persist(region2);
        persister.persist(region1);

        ASSERT_CHECK_EQUAL(persister._getFiles().size(), 1, suc);
    }

    {
        dir.remove(true);
        dir.createDirectory();

        RegionPersister::Config config;
        config.file_size                    = region_ser_size + 1; // two region per file.
        config.merge_hint_low_used_file_num = 1;                   // do merge even there is only one low used file.

        RegionMap regions;
        regions.emplace(1, region1);
        regions.emplace(2, region2);
        regions.emplace(3, region3);

        {
            RegionPersister persister(dir_path, config);
            persister.persist(region1);
            persister.persist(region1);
            persister.persist(region1);
            persister.persist(region2);
            persister.persist(region3);
            persister.persist(region1);
            persister.persist(region3);
            persister.persist(region2);
            persister.persist(region1);
            persister.persist(region1);

            auto size = persister._getFiles().size();
            ASSERT_CHECK_EQUAL(size, 5, suc);
        }

        {
            RegionPersister persister(dir_path, config);
            RegionMap restore_regions;
            persister.restore(restore_regions);
            ASSERT_CHECK_EQUAL(3, restore_regions.size(), suc);

            persister.gc();

            ASSERT_CHECK_EQUAL(persister._getFiles().size(), 2, suc);
        }
    }

    {
        dir.remove(true);
        dir.createDirectory();

        RegionPersister::Config config;
        config.file_size                    = 1; // one region per file
        config.merge_hint_low_used_file_num = 1; // do merge even there is only one low used file.

        RegionMap regions;
        regions.emplace(1, region1);
        regions.emplace(2, region2);
        regions.emplace(3, region3);

        RegionPersister persister(dir_path, config);
        persister.persist(region1);
        persister.persist(region1);
        persister.persist(region1);

        ASSERT_CHECK_EQUAL(persister._getFiles().size(), 3, suc);

        persister.gc();

        // remove the other 2 files, except the current file.
        ASSERT_CHECK_EQUAL(persister._getFiles().size(), 1, suc);

        persister.persist(region2);
        persister.persist(region3);

        ASSERT_CHECK_EQUAL(persister._getFiles().size(), 3, suc);

        persister.gc();

        ASSERT_CHECK_EQUAL(persister._getFiles().size(), 2, suc);
    }

    return suc ? 0 : 1;
}
