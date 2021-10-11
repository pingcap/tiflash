#pragma once

namespace DB
{
namespace CharUtil
{
// upper and lower, same as tidb.
const auto UpperCase = 0;
const auto LowerCase = 1;
const auto TitleCase = 2;
const auto MaxCase = 3;

const Int32 MaxRune = 1114111; // Maximum valid Unicode code point, '\U0010FFFF'
const Int32 ReplacementChar = 65533; // Represents invalid code points, "\uFFFD"

const auto UpperLower = MaxRune + 1; // (Cannot be a valid delta.)

struct CaseRange
{
    UInt32 lo;
    UInt32 hi;
    Int32 delta[MaxCase];
};

const CaseRange caseRange[]{
    {0x0041, 0x005A, {0, 32, 0}},
    {0x0061, 0x007A, {-32, 0, -32}},
    {0x00B5, 0x00B5, {743, 0, 743}},
    {0x00C0, 0x00D6, {0, 32, 0}},
    {0x00D8, 0x00DE, {0, 32, 0}},
    {0x00E0, 0x00F6, {-32, 0, -32}},
    {0x00F8, 0x00FE, {-32, 0, -32}},
    {0x00FF, 0x00FF, {121, 0, 121}},
    {0x0100, 0x012F, {UpperLower, UpperLower, UpperLower}},
    {0x0130, 0x0130, {0, -199, 0}},
    {0x0131, 0x0131, {-232, 0, -232}},
    {0x0132, 0x0137, {UpperLower, UpperLower, UpperLower}},
    {0x0139, 0x0148, {UpperLower, UpperLower, UpperLower}},
    {0x014A, 0x0177, {UpperLower, UpperLower, UpperLower}},
    {0x0178, 0x0178, {0, -121, 0}},
    {0x0179, 0x017E, {UpperLower, UpperLower, UpperLower}},
    {0x017F, 0x017F, {-300, 0, -300}},
    {0x0180, 0x0180, {195, 0, 195}},
    {0x0181, 0x0181, {0, 210, 0}},
    {0x0182, 0x0185, {UpperLower, UpperLower, UpperLower}},
    {0x0186, 0x0186, {0, 206, 0}},
    {0x0187, 0x0188, {UpperLower, UpperLower, UpperLower}},
    {0x0189, 0x018A, {0, 205, 0}},
    {0x018B, 0x018C, {UpperLower, UpperLower, UpperLower}},
    {0x018E, 0x018E, {0, 79, 0}},
    {0x018F, 0x018F, {0, 202, 0}},
    {0x0190, 0x0190, {0, 203, 0}},
    {0x0191, 0x0192, {UpperLower, UpperLower, UpperLower}},
    {0x0193, 0x0193, {0, 205, 0}},
    {0x0194, 0x0194, {0, 207, 0}},
    {0x0195, 0x0195, {97, 0, 97}},
    {0x0196, 0x0196, {0, 211, 0}},
    {0x0197, 0x0197, {0, 209, 0}},
    {0x0198, 0x0199, {UpperLower, UpperLower, UpperLower}},
    {0x019A, 0x019A, {163, 0, 163}},
    {0x019C, 0x019C, {0, 211, 0}},
    {0x019D, 0x019D, {0, 213, 0}},
    {0x019E, 0x019E, {130, 0, 130}},
    {0x019F, 0x019F, {0, 214, 0}},
    {0x01A0, 0x01A5, {UpperLower, UpperLower, UpperLower}},
    {0x01A6, 0x01A6, {0, 218, 0}},
    {0x01A7, 0x01A8, {UpperLower, UpperLower, UpperLower}},
    {0x01A9, 0x01A9, {0, 218, 0}},
    {0x01AC, 0x01AD, {UpperLower, UpperLower, UpperLower}},
    {0x01AE, 0x01AE, {0, 218, 0}},
    {0x01AF, 0x01B0, {UpperLower, UpperLower, UpperLower}},
    {0x01B1, 0x01B2, {0, 217, 0}},
    {0x01B3, 0x01B6, {UpperLower, UpperLower, UpperLower}},
    {0x01B7, 0x01B7, {0, 219, 0}},
    {0x01B8, 0x01B9, {UpperLower, UpperLower, UpperLower}},
    {0x01BC, 0x01BD, {UpperLower, UpperLower, UpperLower}},
    {0x01BF, 0x01BF, {56, 0, 56}},
    {0x01C4, 0x01C4, {0, 2, 1}},
    {0x01C5, 0x01C5, {-1, 1, 0}},
    {0x01C6, 0x01C6, {-2, 0, -1}},
    {0x01C7, 0x01C7, {0, 2, 1}},
    {0x01C8, 0x01C8, {-1, 1, 0}},
    {0x01C9, 0x01C9, {-2, 0, -1}},
    {0x01CA, 0x01CA, {0, 2, 1}},
    {0x01CB, 0x01CB, {-1, 1, 0}},
    {0x01CC, 0x01CC, {-2, 0, -1}},
    {0x01CD, 0x01DC, {UpperLower, UpperLower, UpperLower}},
    {0x01DD, 0x01DD, {-79, 0, -79}},
    {0x01DE, 0x01EF, {UpperLower, UpperLower, UpperLower}},
    {0x01F1, 0x01F1, {0, 2, 1}},
    {0x01F2, 0x01F2, {-1, 1, 0}},
    {0x01F3, 0x01F3, {-2, 0, -1}},
    {0x01F4, 0x01F5, {UpperLower, UpperLower, UpperLower}},
    {0x01F6, 0x01F6, {0, -97, 0}},
    {0x01F7, 0x01F7, {0, -56, 0}},
    {0x01F8, 0x021F, {UpperLower, UpperLower, UpperLower}},
    {0x0220, 0x0220, {0, -130, 0}},
    {0x0222, 0x0233, {UpperLower, UpperLower, UpperLower}},
    {0x023A, 0x023A, {0, 10795, 0}},
    {0x023B, 0x023C, {UpperLower, UpperLower, UpperLower}},
    {0x023D, 0x023D, {0, -163, 0}},
    {0x023E, 0x023E, {0, 10792, 0}},
    {0x023F, 0x0240, {10815, 0, 10815}},
    {0x0241, 0x0242, {UpperLower, UpperLower, UpperLower}},
    {0x0243, 0x0243, {0, -195, 0}},
    {0x0244, 0x0244, {0, 69, 0}},
    {0x0245, 0x0245, {0, 71, 0}},
    {0x0246, 0x024F, {UpperLower, UpperLower, UpperLower}},
    {0x0250, 0x0250, {10783, 0, 10783}},
    {0x0251, 0x0251, {10780, 0, 10780}},
    {0x0252, 0x0252, {10782, 0, 10782}},
    {0x0253, 0x0253, {-210, 0, -210}},
    {0x0254, 0x0254, {-206, 0, -206}},
    {0x0256, 0x0257, {-205, 0, -205}},
    {0x0259, 0x0259, {-202, 0, -202}},
    {0x025B, 0x025B, {-203, 0, -203}},
    {0x025C, 0x025C, {42319, 0, 42319}},
    {0x0260, 0x0260, {-205, 0, -205}},
    {0x0261, 0x0261, {42315, 0, 42315}},
    {0x0263, 0x0263, {-207, 0, -207}},
    {0x0265, 0x0265, {42280, 0, 42280}},
    {0x0266, 0x0266, {42308, 0, 42308}},
    {0x0268, 0x0268, {-209, 0, -209}},
    {0x0269, 0x0269, {-211, 0, -211}},
    {0x026A, 0x026A, {42308, 0, 42308}},
    {0x026B, 0x026B, {10743, 0, 10743}},
    {0x026C, 0x026C, {42305, 0, 42305}},
    {0x026F, 0x026F, {-211, 0, -211}},
    {0x0271, 0x0271, {10749, 0, 10749}},
    {0x0272, 0x0272, {-213, 0, -213}},
    {0x0275, 0x0275, {-214, 0, -214}},
    {0x027D, 0x027D, {10727, 0, 10727}},
    {0x0280, 0x0280, {-218, 0, -218}},
    {0x0282, 0x0282, {42307, 0, 42307}},
    {0x0283, 0x0283, {-218, 0, -218}},
    {0x0287, 0x0287, {42282, 0, 42282}},
    {0x0288, 0x0288, {-218, 0, -218}},
    {0x0289, 0x0289, {-69, 0, -69}},
    {0x028A, 0x028B, {-217, 0, -217}},
    {0x028C, 0x028C, {-71, 0, -71}},
    {0x0292, 0x0292, {-219, 0, -219}},
    {0x029D, 0x029D, {42261, 0, 42261}},
    {0x029E, 0x029E, {42258, 0, 42258}},
    {0x0345, 0x0345, {84, 0, 84}},
    {0x0370, 0x0373, {UpperLower, UpperLower, UpperLower}},
    {0x0376, 0x0377, {UpperLower, UpperLower, UpperLower}},
    {0x037B, 0x037D, {130, 0, 130}},
    {0x037F, 0x037F, {0, 116, 0}},
    {0x0386, 0x0386, {0, 38, 0}},
    {0x0388, 0x038A, {0, 37, 0}},
    {0x038C, 0x038C, {0, 64, 0}},
    {0x038E, 0x038F, {0, 63, 0}},
    {0x0391, 0x03A1, {0, 32, 0}},
    {0x03A3, 0x03AB, {0, 32, 0}},
    {0x03AC, 0x03AC, {-38, 0, -38}},
    {0x03AD, 0x03AF, {-37, 0, -37}},
    {0x03B1, 0x03C1, {-32, 0, -32}},
    {0x03C2, 0x03C2, {-31, 0, -31}},
    {0x03C3, 0x03CB, {-32, 0, -32}},
    {0x03CC, 0x03CC, {-64, 0, -64}},
    {0x03CD, 0x03CE, {-63, 0, -63}},
    {0x03CF, 0x03CF, {0, 8, 0}},
    {0x03D0, 0x03D0, {-62, 0, -62}},
    {0x03D1, 0x03D1, {-57, 0, -57}},
    {0x03D5, 0x03D5, {-47, 0, -47}},
    {0x03D6, 0x03D6, {-54, 0, -54}},
    {0x03D7, 0x03D7, {-8, 0, -8}},
    {0x03D8, 0x03EF, {UpperLower, UpperLower, UpperLower}},
    {0x03F0, 0x03F0, {-86, 0, -86}},
    {0x03F1, 0x03F1, {-80, 0, -80}},
    {0x03F2, 0x03F2, {7, 0, 7}},
    {0x03F3, 0x03F3, {-116, 0, -116}},
    {0x03F4, 0x03F4, {0, -60, 0}},
    {0x03F5, 0x03F5, {-96, 0, -96}},
    {0x03F7, 0x03F8, {UpperLower, UpperLower, UpperLower}},
    {0x03F9, 0x03F9, {0, -7, 0}},
    {0x03FA, 0x03FB, {UpperLower, UpperLower, UpperLower}},
    {0x03FD, 0x03FF, {0, -130, 0}},
    {0x0400, 0x040F, {0, 80, 0}},
    {0x0410, 0x042F, {0, 32, 0}},
    {0x0430, 0x044F, {-32, 0, -32}},
    {0x0450, 0x045F, {-80, 0, -80}},
    {0x0460, 0x0481, {UpperLower, UpperLower, UpperLower}},
    {0x048A, 0x04BF, {UpperLower, UpperLower, UpperLower}},
    {0x04C0, 0x04C0, {0, 15, 0}},
    {0x04C1, 0x04CE, {UpperLower, UpperLower, UpperLower}},
    {0x04CF, 0x04CF, {-15, 0, -15}},
    {0x04D0, 0x052F, {UpperLower, UpperLower, UpperLower}},
    {0x0531, 0x0556, {0, 48, 0}},
    {0x0561, 0x0586, {-48, 0, -48}},
    {0x10A0, 0x10C5, {0, 7264, 0}},
    {0x10C7, 0x10C7, {0, 7264, 0}},
    {0x10CD, 0x10CD, {0, 7264, 0}},
    {0x10D0, 0x10FA, {3008, 0, 0}},
    {0x10FD, 0x10FF, {3008, 0, 0}},
    {0x13A0, 0x13EF, {0, 38864, 0}},
    {0x13F0, 0x13F5, {0, 8, 0}},
    {0x13F8, 0x13FD, {-8, 0, -8}},
    {0x1C80, 0x1C80, {-6254, 0, -6254}},
    {0x1C81, 0x1C81, {-6253, 0, -6253}},
    {0x1C82, 0x1C82, {-6244, 0, -6244}},
    {0x1C83, 0x1C84, {-6242, 0, -6242}},
    {0x1C85, 0x1C85, {-6243, 0, -6243}},
    {0x1C86, 0x1C86, {-6236, 0, -6236}},
    {0x1C87, 0x1C87, {-6181, 0, -6181}},
    {0x1C88, 0x1C88, {35266, 0, 35266}},
    {0x1C90, 0x1CBA, {0, -3008, 0}},
    {0x1CBD, 0x1CBF, {0, -3008, 0}},
    {0x1D79, 0x1D79, {35332, 0, 35332}},
    {0x1D7D, 0x1D7D, {3814, 0, 3814}},
    {0x1D8E, 0x1D8E, {35384, 0, 35384}},
    {0x1E00, 0x1E95, {UpperLower, UpperLower, UpperLower}},
    {0x1E9B, 0x1E9B, {-59, 0, -59}},
    {0x1E9E, 0x1E9E, {0, -7615, 0}},
    {0x1EA0, 0x1EFF, {UpperLower, UpperLower, UpperLower}},
    {0x1F00, 0x1F07, {8, 0, 8}},
    {0x1F08, 0x1F0F, {0, -8, 0}},
    {0x1F10, 0x1F15, {8, 0, 8}},
};

inline int toCase(int _case, int ch)
{
    if (_case < 0 || MaxCase <= _case)
    {
        return ReplacementChar;
    }
    // binary search over ranges
    int lo = 0;
    int hi = sizeof(caseRange) / sizeof(CaseRange);
    while (lo < hi)
    {
        auto m = lo + (hi - lo) / 2;
        auto cr = caseRange[m];
        if (Int32(cr.lo) <= ch && ch <= Int32(cr.hi))
        {
            auto delta = cr.delta[_case];
            if (delta > MaxRune)
            {
                // In an Upper-Lower sequence, which always starts with
                // an UpperCase letter, the real deltas always look like:
                //	{0, 1, 0}    UpperCase (Lower is next)
                //	{-1, 0, -1}  LowerCase (Upper, Title are previous)
                // The characters at even offsets from the beginning of the
                // sequence are upper case; the ones at odd offsets are lower.
                // The correct mapping can be done by clearing or setting the low
                // bit in the sequence offset.
                // The constants UpperCase and TitleCase are even while LowerCase
                // is odd so we take the low bit from _case.
                return Int32(cr.lo) + (((ch - Int32(cr.lo)) & (~1)) | Int32(_case & 1));
            }
            return ch + delta;
        }
        if (ch < Int32(cr.lo))
        {
            hi = m;
        }
        else
        {
            lo = m + 1;
        }
    }
    return ch;
}

inline int unicodeToUpper(int ch)
{
    return toCase(UpperCase, ch);
}

inline int unicodeToLower(int ch)
{
    return toCase(LowerCase, ch);
}

} // namespace CharUtil
} // namespace DB
