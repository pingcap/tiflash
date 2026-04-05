// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Storages/DeltaMerge/Index/InvertedIndex/Reader/ReaderFromColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader/ReaderFromDMFile.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader/ReaderFromSegment.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM
{

BitmapFilterPtr InvertedIndexReaderFromSegment::loadStable(
    const SegmentSnapshotPtr & snapshot,
    const ColumnRangePtr & column_range,
    const LocalIndexCachePtr & local_index_cache,
    const ScanContextPtr & scan_context)
{
    InvertedIndexReaderFromSegment reader(snapshot, column_range, local_index_cache, scan_context);
    return reader.loadStableImpl();
}

BitmapFilterPtr InvertedIndexReaderFromSegment::loadDelta(
    const SegmentSnapshotPtr & snapshot,
    const ColumnRangePtr & column_range,
    const LocalIndexCachePtr & local_index_cache,
    const ScanContextPtr & scan_context)
{
    InvertedIndexReaderFromSegment reader(snapshot, column_range, local_index_cache, scan_context);
    return reader.loadDeltaImpl();
}

BitmapFilterPtr InvertedIndexReaderFromSegment::loadStableImpl()
{
    BitmapFilterPtr bitmap_filter = std::make_shared<BitmapFilter>(0, false);
    for (const auto & dmfile : snapshot->stable->getDMFiles())
    {
        InvertedIndexReaderFromDMFile reader(column_range, dmfile, local_index_cache, scan_context);
        if (auto sub_bf = reader.load(); sub_bf)
            bitmap_filter->append(*sub_bf);
        else
            bitmap_filter->append(BitmapFilter(dmfile->getRows(), true));
    }
    return bitmap_filter;
}

BitmapFilterPtr InvertedIndexReaderFromSegment::loadDeltaImpl()
{
    BitmapFilterPtr bitmap_filter = std::make_shared<BitmapFilter>(0, false);
    const auto & persisted_snap = snapshot->delta->getPersistedFileSetSnapshot();
    const auto & data_provider = persisted_snap->getDataProvider();
    for (const auto & cf : persisted_snap->getColumnFiles())
    {
        if (auto * tiny_cf = cf->tryToTinyFile(); tiny_cf)
        {
            InvertedIndexReaderFromColumnFileTiny
                reader(column_range, *tiny_cf, data_provider, local_index_cache, scan_context);
            if (auto sub_bf = reader.load(); sub_bf)
                bitmap_filter->append(*sub_bf);
            else
                bitmap_filter->append(BitmapFilter(tiny_cf->getRows(), true));
        }
        else
        {
            bitmap_filter->append(BitmapFilter(cf->getRows(), true));
        }
    }
    const auto & memtable_snap = snapshot->delta->getMemTableSetSnapshot();
    for (const auto & cf : memtable_snap->getColumnFiles())
    {
        bitmap_filter->append(BitmapFilter(cf->getRows(), true));
    }
    return bitmap_filter;
}

} // namespace DB::DM
