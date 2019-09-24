#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionsRangeIndex.h>
#include "region_helper.h"

using namespace DB;

RegionPtr makeRegion(UInt64 id, const std::string start_key, const std::string end_key)
{
    return std::make_shared<Region>(
        RegionMeta(createPeer(666, true), createRegionInfo(id, std::move(start_key), std::move(end_key)), initialApplyState()));
}

int main(int, char **)
{
    {
        RegionsRangeIndex region_index;
        const auto & root_map = region_index.getRoot();
        assert(root_map.size() == 2);

        region_index.add(makeRegion(1, "", ""));

        assert(root_map.begin()->second.region_map.size() == 1);

        region_index.add(makeRegion(2, "", RecordKVFormat::genKey(1, 3)));
        region_index.add(makeRegion(3, "", RecordKVFormat::genKey(1, 1)));

        auto res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        assert(res.size() == 3);

        region_index.add(makeRegion(4, RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 4)));

        assert(root_map.size() == 5);

        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        assert(res.size() == 4);

        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), TiKVKey("")));
        assert(res.size() == 3);

        res = region_index.findByRangeOverlap(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 5)));
        assert(res.size() == 3);
        assert(res.find(1) != res.end());
        assert(res.find(2) != res.end());
        assert(res.find(4) != res.end());

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 4)), 4);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        assert(res.size() == 3);

        region_index.remove(RegionRangeKeys::makeComparableKeys(TiKVKey(), RecordKVFormat::genKey(1, 1)), 3);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        assert(res.size() == 2);

        region_index.remove(RegionRangeKeys::makeComparableKeys(TiKVKey(), RecordKVFormat::genKey(1, 3)), 2);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        assert(res.size() == 1);

        region_index.remove(RegionRangeKeys::makeComparableKeys(TiKVKey(), TiKVKey()), 1);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        assert(res.size() == 0);

        assert(root_map.size() == 2);
    }

    {
        RegionsRangeIndex region_index;
        const auto & root_map = region_index.getRoot();
        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(TiKVKey(), TiKVKey()), 1);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            assert(res == "[RegionsRangeIndex::remove] not found region 1");
        }

        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 5)));
        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 4), RecordKVFormat::genKey(1, 5)), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            assert(res == "[RegionsRangeIndex::remove] not found start key");
        }

        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 4)), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            assert(res == "[RegionsRangeIndex::remove] not found end key");
        }

        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 3)), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            assert(res == "[RegionsRangeIndex::remove] range of region 2 is empty");
        }

        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), TiKVKey()), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            assert(res == "[RegionsRangeIndex::remove] not found region 2");
        }

        region_index.clear();

        try
        {
            region_index.add(makeRegion(6, RecordKVFormat::genKey(6, 6), RecordKVFormat::genKey(6, 6)));
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            assert(res == "[RegionsRangeIndex::add] range of region 6 is empty");
        }

        region_index.clear();

        region_index.add(makeRegion(1, TiKVKey(), RecordKVFormat::genKey(1, 1)));
        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 2)));
        region_index.add(makeRegion(3, RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 3)));

        assert(root_map.size() == 5);
        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 3)), 3);
        assert(root_map.size() == 4);

        region_index.remove(RegionRangeKeys::makeComparableKeys(TiKVKey(), RecordKVFormat::genKey(1, 1)), 1);
        assert(root_map.size() == 4);

        region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 2)), 2);
        assert(root_map.size() == 2);
    }

    return 0;
}
