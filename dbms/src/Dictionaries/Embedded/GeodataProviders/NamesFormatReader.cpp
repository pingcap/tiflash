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

#include <Dictionaries/Embedded/GeodataProviders/NamesFormatReader.h>

#include <IO/ReadHelpers.h>


bool LanguageRegionsNamesFormatReader::readNext(RegionNameEntry & entry)
{
    while (!input->eof())
    {
        Int32 read_region_id;
        std::string region_name;

        DB::readIntText(read_region_id, *input);
        DB::assertChar('\t', *input);
        DB::readString(region_name, *input);
        DB::assertChar('\n', *input);

        if (read_region_id <= 0)
            continue;

        entry.id = read_region_id;
        entry.name = region_name;
        return true;
    }

    return false;
}
