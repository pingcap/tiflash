// Copyright 2023 PingCAP, Inc.
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

#include <Dictionaries/Embedded/GeodataProviders/HierarchyFormatReader.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


bool RegionsHierarchyFormatReader::readNext(RegionEntry & entry)
{
    while (!input->eof())
    {
        /** Our internal geobase has negative numbers,
            *  that means "this is garbage, ignore this row".
            */
        Int32 read_region_id = 0;
        Int32 read_parent_id = 0;
        Int8 read_type = 0;

        DB::readIntText(read_region_id, *input);
        DB::assertChar('\t', *input);
        DB::readIntText(read_parent_id, *input);
        DB::assertChar('\t', *input);
        DB::readIntText(read_type, *input);

        /** Then there can be a newline (old version)
            *  or tab, the region's population, line feed (new version).
            */
        RegionPopulation population = 0;
        if (!input->eof() && *input->position() == '\t')
        {
            ++input->position();
            UInt64 population_big = 0;
            DB::readIntText(population_big, *input);
            population = population_big > std::numeric_limits<RegionPopulation>::max()
                ? std::numeric_limits<RegionPopulation>::max()
                : population_big;
        }
        DB::assertChar('\n', *input);

        if (read_region_id <= 0 || read_type < 0)
            continue;

        RegionID region_id = read_region_id;
        RegionID parent_id = 0;

        if (read_parent_id >= 0)
            parent_id = read_parent_id;

        RegionType type = static_cast<RegionType>(read_type);

        entry.id = region_id;
        entry.parent_id = parent_id;
        entry.type = type;
        entry.population = population;
        return true;
    }

    return false;
}
