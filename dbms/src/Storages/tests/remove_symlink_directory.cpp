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

#include <port/unistd.h>
#include <iostream>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Common/Exception.h>


int main(int, char **)
try
{
    Poco::File dir("./test_dir/");
    dir.createDirectories();

    Poco::File("./test_dir/file").createFile();

    if (0 != symlink("./test_dir", "./test_link"))
        DB::throwFromErrno("Cannot create symlink");

    Poco::File link("./test_link");
    link.renameTo("./test_link2");

    Poco::File("./test_link2").remove(true);
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(false) << "\n";
    return 1;
}
