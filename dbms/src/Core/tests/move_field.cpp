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

#include <iostream>
#include <Core/Field.h>


int main(int, char **)
{
    using namespace DB;

    Field f;

    f = Field{String{"Hello, world"}};
    std::cerr << f.get<String>() << "\n";
    f = Field{String{"Hello, world!"}};
    std::cerr << f.get<String>() << "\n";
    f = Field{Array{Field{String{"Hello, world!!"}}}};
    std::cerr << f.get<Array>()[0].get<String>() << "\n";
    f = String{"Hello, world!!!"};
    std::cerr << f.get<String>() << "\n";
    f = Array{Field{String{"Hello, world!!!!"}}};
    std::cerr << f.get<Array>()[0].get<String>() << "\n";
    f = Array{String{"Hello, world!!!!!"}};
    std::cerr << f.get<Array>()[0].get<String>() << "\n";

    return 0;
}
