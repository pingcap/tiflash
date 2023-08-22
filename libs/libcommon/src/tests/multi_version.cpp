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

#include <Poco/Exception.h>
#include <common/MultiVersion.h>
#include <common/ThreadPool.h>
#include <string.h>

#include <functional>
#include <iostream>


using T = std::string;
using MV = MultiVersion<T>;
using Results = std::vector<T>;


void thread1(MV & x, T & result)
{
    MV::Version v = x.get();
    result = *v;
}

void thread2(MV & x, const char * result)
{
    x.set(std::make_unique<T>(result));
}


int main(int argc, char ** argv)
{
    try
    {
        const char * s1 = "Hello!";
        const char * s2 = "Goodbye!";

        size_t n = 1000;
        MV x(std::make_unique<T>(s1));
        Results results(n);

        ThreadPool tp(8);
        for (size_t i = 0; i < n; ++i)
        {
            tp.schedule(std::bind(thread1, std::ref(x), std::ref(results[i])));
            tp.schedule(std::bind(thread2, std::ref(x), (rand() % 2) ? s1 : s2));
        }
        tp.wait();

        for (size_t i = 0; i < n; ++i)
            std::cerr << results[i] << " ";
        std::cerr << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.message() << std::endl;
        throw;
    }

    return 0;
}
