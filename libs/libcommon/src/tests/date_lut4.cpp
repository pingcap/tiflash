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

#include <common/DateLUT.h>

#include <iostream>


int main(int argc, char ** argv)
{
    /** В DateLUT был глюк - для времён из дня 1970-01-01, возвращался номер часа больше 23. */
    static const time_t TIME = 66130;

    const auto & date_lut = DateLUT::instance();

    std::cerr << date_lut.toHour(TIME) << std::endl;
    std::cerr << date_lut.toDayNum(TIME) << std::endl;

    const auto * values = reinterpret_cast<const DateLUTImpl::Values *>(&date_lut);

    std::cerr << values[0].date << ", " << time_t(values[1].date - values[0].date) << std::endl;

    return 0;
}
