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

#include <Parsers/ASTSampleRatio.h>

namespace DB
{


String ASTSampleRatio::toString(BigNum num)
{
    if (num == 0)
        return "0";

    static const size_t MAX_WIDTH = 40;

    char tmp[MAX_WIDTH];

    char * pos;
    for (pos = tmp + MAX_WIDTH - 1; num != 0; --pos)
    {
        *pos = '0' + num % 10;
        num /= 10;
    }

    ++pos;

    return String(pos, tmp + MAX_WIDTH - pos);
}


String ASTSampleRatio::toString(Rational ratio)
{
    if (ratio.denominator == 1)
        return toString(ratio.numerator);
    else
        return toString(ratio.numerator) + " / " + toString(ratio.denominator);
}




}
