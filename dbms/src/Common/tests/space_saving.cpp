// Copyright 2022 PingCAP, Ltd.
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

#include <Common/SpaceSaving.h>
#include <common/StringRef.h>

#include <iomanip>
#include <iostream>
#include <map>
#include <string>

int main(int, char **)
{
    {
        using Cont = DB::SpaceSaving<int>;
        Cont first(10);

        /* Test biased insertion */

        for (int i = 0; i < 200; ++i)
        {
            first.insert(i);
            int k = i % 5; // Bias towards 0-4
            first.insert(k);
        }

        /* Test whether the biased elements are retained */

        std::map<int, UInt64> expect;
        for (int i = 0; i < 5; ++i)
        {
            expect[i] = 41;
        }

        for (auto x : first.topK(5))
        {
            if (expect[x.key] != x.count)
            {
                std::cerr << "key: " << x.key << " value: " << x.count << " expected: " << expect[x.key] << std::endl;
            }
            else
            {
                std::cout << "key: " << x.key << " value: " << x.count << std::endl;
            }
            expect.erase(x.key);
        }

        if (!expect.empty())
        {
            std::cerr << "expected to find all heavy hitters" << std::endl;
        }

        /* Create another table and test merging */

        Cont second(10);
        for (int i = 0; i < 200; ++i)
        {
            first.insert(i);
        }

        for (int i = 0; i < 5; ++i)
        {
            expect[i] = 42;
        }

        first.merge(second);

        for (auto x : first.topK(5))
        {
            if (expect[x.key] != x.count)
            {
                std::cerr << "key: " << x.key << " value: " << x.count << " expected: " << expect[x.key] << std::endl;
            }
            else
            {
                std::cout << "key: " << x.key << " value: " << x.count << std::endl;
            }
            expect.erase(x.key);
        }
    }

    {
        /* Same test for string keys */

        using Cont = DB::SpaceSaving<StringRef, StringRefHash>;
        Cont cont(10);

        for (int i = 0; i < 400; ++i)
        {
            cont.insert(std::to_string(i));
            cont.insert(std::to_string(i % 5)); // Bias towards 0-4
        }

        // The hashing is going to be more lossy
        // Expect at least ~ 10% count
        std::map<std::string, UInt64> expect;
        for (int i = 0; i < 5; ++i)
        {
            expect[std::to_string(i)] = 38;
        }

        for (auto x : cont.topK(5))
        {
            auto key = x.key.toString();
            if (x.count < expect[key])
            {
                std::cerr << "key: " << key << " value: " << x.count << " expected: " << expect[key] << std::endl;
            }
            else
            {
                std::cout << "key: " << key << " value: " << x.count << std::endl;
            }
            expect.erase(key);
        }

        if (!expect.empty())
        {
            std::cerr << "expected to find all heavy hitters" << std::endl;
            abort();
        }
    }

    return 0;
}
