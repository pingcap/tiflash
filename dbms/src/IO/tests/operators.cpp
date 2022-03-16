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

#include <IO/Operators.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>


int main(int, char **)
{
    {
        DB::WriteBufferFromFileDescriptor buf(STDOUT_FILENO);
        buf
            << "Hello, world!" << '\n'
            << DB::escape << "Hello, world!" << '\n'
            << DB::quote << "Hello, world!" << '\n'
            << DB::double_quote << "Hello, world!" << '\n'
            << DB::binary << "Hello, world!" << '\n'
            << LocalDateTime(time(nullptr)) << '\n'
            << LocalDate(time(nullptr)) << '\n'
            << 1234567890123456789UL << '\n'
            << DB::flush;
    }

    {
        std::string hello;
        {
            DB::WriteBufferFromString buf(hello);
            buf << "Hello";
            std::cerr << hello.size() << '\n';
        }

        std::cerr << hello.size() << '\n';
        std::cerr << hello << '\n';
    }

    return 0;
}
