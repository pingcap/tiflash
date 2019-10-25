#pragma once

#include <Core/Types.h>
#include <common/DateLUT.h>
#include <common/DateLUTImpl.h>

namespace DB
{

const static Int32 MAX_WORD_BUF_LEN = 9;
const static Int32 DIGITS_PER_WORD = 9;
const static Int32 POWERS10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

class TiDBDecimal
{
public:
    TiDBDecimal(UInt32 scale, const std::vector<Int32> & digits, bool neg);

    Int8 digits_int;
    Int8 digits_frac;
    Int8 result_frac;
    bool negative;
    Int32 word_buf[MAX_WORD_BUF_LEN] = {0};
};
} // namespace DB
