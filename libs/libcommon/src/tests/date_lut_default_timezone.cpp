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
#include <common/DateLUT.h>

#include <iostream>

int main(int argc, char ** argv)
{
    try
    {
        const auto & date_lut = DateLUT::instance();
        std::cout << "Detected default timezone: `" << date_lut.getTimeZone() << "'" << std::endl;
        time_t now = time(NULL);
        std::cout << "Current time: " << date_lut.timeToString(now)
                  << ", UTC: " << DateLUT::instance("UTC").timeToString(now) << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
        return 1;
    }
    catch (std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        return 2;
    }
    catch (...)
    {
        std::cerr << "Some exception" << std::endl;
        return 3;
    }
    return 0;
}
