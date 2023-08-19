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

#include <string>

#include <IO/parseDateTimeBestEffort.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>


using namespace DB;

int main(int, char **)
try
{
    const DateLUTImpl & local_time_zone = DateLUT::instance();
    const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    time_t res;
    parseDateTimeBestEffort(res, in, local_time_zone, utc_time_zone);
    writeDateTimeText(res, out);
    writeChar('\n', out);

    return 0;
}
catch (const Exception & e)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
