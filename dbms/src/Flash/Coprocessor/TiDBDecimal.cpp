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

#include <Flash/Coprocessor/TiDBDecimal.h>

namespace DB
{
Int32 vectorToInt(int start_index, int end_index, const std::vector<Int32> & vec)
{
    Int32 ret = 0;
    for (int i = end_index - 1; i >= start_index; i--)
    {
        ret = ret * 10 + vec[i];
    }
    return ret;
}

TiDBDecimal::TiDBDecimal(UInt32 scale, const std::vector<Int32> & digits, bool neg)
    : negative(neg)
{
    UInt32 prec = digits.size();
    if (prec == 0)
    {
        // zero decimal
        digits_int = digits_frac = result_frac = 0;
    }
    else
    {
        digits_int = prec - scale;
        digits_frac = scale;
        result_frac = scale;

        int word_int = digits_int / DIGITS_PER_WORD;
        int leading_digit = digits_int % DIGITS_PER_WORD;

        int word_frac = digits_frac / DIGITS_PER_WORD;
        int tailing_digit = digits_frac % DIGITS_PER_WORD;

        int word_index = 0;
        Int32 value = 0;
        int vector_index = digits.size();

        // fill the int part
        if (leading_digit > 0)
        {
            value = vectorToInt(vector_index - leading_digit, vector_index, digits);
            vector_index -= leading_digit;
            if (value > 0)
            {
                word_buf[word_index++] = value;
            }
            else
            {
                digits_int -= leading_digit;
            }
        }
        for (int i = 0; i < word_int; i++, vector_index -= DIGITS_PER_WORD)
        {
            value = vectorToInt(vector_index - DIGITS_PER_WORD, vector_index, digits);
            if (word_index > 0 || value > 0)
            {
                word_buf[word_index++] = value;
            }
            else
            {
                digits_int -= DIGITS_PER_WORD;
            }
        }

        // fill the frac part
        for (int i = 0; i < word_frac; i++, vector_index -= DIGITS_PER_WORD)
        {
            value = vectorToInt(vector_index - DIGITS_PER_WORD, vector_index, digits);
            word_buf[word_index++] = value;
        }

        if (tailing_digit > 0)
        {
            value = vectorToInt(vector_index - tailing_digit, vector_index, digits);
            word_buf[word_index++] = value * POWERS10[DIGITS_PER_WORD - tailing_digit];
        }
    }
}
} // namespace DB
