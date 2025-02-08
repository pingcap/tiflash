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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyInvertedIndexReader.h>
#include <Storages/DeltaMerge/File/DMFileInvertedIndexReader.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentInvertedIndexReader.h>

namespace DB::DM
{

BitmapFilterPtr SegmentInvertedIndexReader::loadStable()
{
    BitmapFilterPtr bitmap_filter = std::make_shared<BitmapFilter>(0, false);
    for (const auto & dmfile : snapshot->stable->getDMFiles())
    {
        DMFileInvertedIndexReader reader(column_value_set, dmfile, local_index_cache);
        if (auto sub_bf = reader.load(); sub_bf)
            bitmap_filter->append(*sub_bf);
        else
            bitmap_filter->append(BitmapFilter(dmfile->getRows(), true));
    }
    return bitmap_filter;
}

BitmapFilterPtr SegmentInvertedIndexReader::loadDelta()
{
    BitmapFilterPtr bitmap_filter = std::make_shared<BitmapFilter>(0, false);
    const auto & persisted_snap = snapshot->delta->getPersistedFileSetSnapshot();
    const auto & data_provider = persisted_snap->getDataProvider();
    for (const auto & cf : persisted_snap->getColumnFiles())
    {
        if (auto * tiny_cf = cf->tryToTinyFile(); tiny_cf)
        {
            ColumnFileTinyInvertedIndexReader reader(*tiny_cf, data_provider, column_value_set, local_index_cache);
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

BitmapFilterPtr SegmentInvertedIndexReader::loadAll()
{
    auto stable_bf = loadStable();
    auto delta_bf = loadDelta();
    LOG_DEBUG(
        log,
        "loadAll: stable_bf: {}/{}, delta_bf: {}/{}",
        stable_bf->count(),
        stable_bf->size(),
        delta_bf->count(),
        delta_bf->size());
    stable_bf->append(*delta_bf);
    return stable_bf;
}

} // namespace DB::DM
