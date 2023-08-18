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

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Interpreters/AggregationCommon.h>

#include <iomanip>
#include <iostream>


int main(int, char **)
{
    {
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;
        Cont cont;

        cont.insert(1);
        cont.insert(2);

        Cont::iterator it;
        bool inserted;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << *it << std::endl;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << *it << std::endl;

        for (auto x : cont)
            std::cerr << x << std::endl;

        DB::WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    {
        using Cont = HashSet<DB::UInt128, DB::TrivialHash>;
        Cont cont;

        DB::WriteBufferFromOwnString wb;
        cont.write(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    return 0;
}
