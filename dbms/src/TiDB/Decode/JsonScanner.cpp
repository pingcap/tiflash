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

#include <TiDB/Decode/JsonScanner.h>

namespace DB
{
bool JsonScanIsSpace(char c)
{
    return (static_cast<unsigned char>(c) <= static_cast<unsigned char>(' '))
        && (c == ' ' || c == '\t' || c == '\r' || c == '\n');
}

bool checkJsonValid(const char * data, size_t length)
{
    JsonScanner scanner;
    for (size_t i = 0; i < length; ++i)
    {
        ++scanner.bytes;
        if (scanner.stepFunc(scanner, data[i]) == JsonScanError)
        {
            return false;
        }
    }
    return (scanner.eof() != JsonScanError);
}

// stateBeginValueOrEmpty is the state after reading `[`.
JsonScanAction stateBeginValueOrEmpty(JsonScanner & scanner, char c)
{
    if (JsonScanIsSpace(c))
    {
        return JsonScanSkipSpace;
    }
    if (c == ']')
    {
        return stateEndValue(scanner, c);
    }
    return stateBeginValue(scanner, c);
}

// stateBeginValue is the state at the beginning of the input.
JsonScanAction stateBeginValue(JsonScanner & scanner, char c)
{
    if (JsonScanIsSpace(c))
    {
        return JsonScanSkipSpace;
    }
    switch (c)
    {
    case '{':
        scanner.stepFunc = stateBeginStringOrEmpty;
        return scanner.pushParseState(c, JsonScanParseObjectKey, JsonScanBeginObject);
    case '[':
        scanner.stepFunc = stateBeginValueOrEmpty;
        return scanner.pushParseState(c, JsonScanParseArrayValue, JsonScanBeginArray);
    case '"':
        scanner.stepFunc = stateInString;
        return JsonScanBeginLiteral;
    case '-':
        scanner.stepFunc = stateNeg;
        return JsonScanBeginLiteral;
    case '0': // beginning of 0.123
        scanner.stepFunc = state0;
        return JsonScanBeginLiteral;
    case 't': // beginning of true
        scanner.stepFunc = stateT;
        return JsonScanBeginLiteral;
    case 'f': // beginning of false
        scanner.stepFunc = stateF;
        return JsonScanBeginLiteral;
    case 'n': // beginning of null
        scanner.stepFunc = stateN;
        return JsonScanBeginLiteral;
    default:
        break;
    }
    if ('1' <= c && c <= '9')
    { // beginning of 1234.5
        scanner.stepFunc = state1;
        return JsonScanBeginLiteral;
    }
    return scanner.genError("looking for beginning of value", c);
}

// stateBeginStringOrEmpty is the state after reading `{`.
JsonScanAction stateBeginStringOrEmpty(JsonScanner & scanner, char c)
{
    if (JsonScanIsSpace(c))
    {
        return JsonScanSkipSpace;
    }
    if (c == '}')
    {
        scanner.parse_state_stack.top() = JsonScanParseObjectValue;
        return stateEndValue(scanner, c);
    }
    return stateBeginString(scanner, c);
}

// stateBeginString is the state after reading `{"key": value,`.
JsonScanAction stateBeginString(JsonScanner & scanner, char c)
{
    if (JsonScanIsSpace(c))
    {
        return JsonScanSkipSpace;
    }
    if (c == '"')
    {
        scanner.stepFunc = stateInString;
        return JsonScanBeginLiteral;
    }
    return scanner.genError("looking for beginning of object key string", c);
}

// stateEndValue is the state after completing a value,
// such as after reading `{}` or `true` or `["x"`.
JsonScanAction stateEndValue(JsonScanner & scanner, char c)
{
    size_t n = scanner.parse_state_stack.size();
    if (n == 0)
    {
        // Completed top-level before the current byte.
        scanner.stepFunc = stateEndTop;
        scanner.end_top = true;
        return stateEndTop(scanner, c);
    }
    if (JsonScanIsSpace(c))
    {
        scanner.stepFunc = stateEndValue;
        return JsonScanSkipSpace;
    }
    switch (scanner.parse_state_stack.top())
    {
    case JsonScanParseObjectKey:
        if (c == ':')
        {
            scanner.parse_state_stack.top() = JsonScanParseObjectValue;
            scanner.stepFunc = stateBeginValue;
            return JsonScanObjectKey;
        }
        return scanner.genError("after object key", c);
    case JsonScanParseObjectValue:
        if (c == ',')
        {
            scanner.parse_state_stack.top() = JsonScanParseObjectKey;
            scanner.stepFunc = stateBeginString;
            return JsonScanObjectValue;
        }
        if (c == '}')
        {
            scanner.popParseState();
            return JsonScanEndObject;
        }
        return scanner.genError("after object key:value pair", c);
    case JsonScanParseArrayValue:
        if (c == ',')
        {
            scanner.stepFunc = stateBeginValue;
            return JsonScanArrayValue;
        }
        if (c == ']')
        {
            scanner.popParseState();
            return JsonScanEndArray;
        }
        return scanner.genError("after array element", c);
    default:
        break;
    }
    return scanner.genError("", c);
}

// stateEndTop is the state after finishing the top-level value,
// such as after reading `{}` or `[1,2,3]`.
// Only space characters should be seen now.
JsonScanAction stateEndTop(JsonScanner & scanner, char c)
{
    if (!JsonScanIsSpace(c))
    {
        // Complain about non-space byte on next call.
        scanner.genError("after top-level value", c);
    }
    return JsonScanEnd;
}

// stateInString is the state after reading `"`.
JsonScanAction stateInString(JsonScanner & scanner, char c)
{
    if (c == '"')
    {
        scanner.stepFunc = stateEndValue;
        return JsonScanContinue;
    }
    if (c == '\\')
    {
        scanner.stepFunc = stateInStringEsc;
        return JsonScanContinue;
    }
    if (static_cast<unsigned char>(c) < 0x20)
    {
        return scanner.genError("in string literal", c);
    }
    return JsonScanContinue;
}

// stateInStringEsc is the state after reading `"\` during a quoted string.
JsonScanAction stateInStringEsc(JsonScanner & scanner, char c)
{
    switch (c)
    {
    case 'b':
    case 'f':
    case 'n':
    case 'r':
    case 't':
    case '\\':
    case '/':
    case '"':
        scanner.stepFunc = stateInString;
        return JsonScanContinue;
    case 'u':
        scanner.stepFunc = stateInStringEscU;
        return JsonScanContinue;
    default:
        break;
    }
    return scanner.genError("in string escape code", c);
}

// stateInStringEscU is the state after reading `"\u` during a quoted string.
JsonScanAction stateInStringEscU(JsonScanner & scanner, char c)
{
    if (('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'))
    {
        scanner.stepFunc = stateInStringEscU1;
        return JsonScanContinue;
    }
    // numbers
    return scanner.genError("in \\u hexadecimal character escape", c);
}

// stateInStringEscU1 is the state after reading `"\u1` during a quoted string.
JsonScanAction stateInStringEscU1(JsonScanner & scanner, char c)
{
    if (('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'))
    {
        scanner.stepFunc = stateInStringEscU12;
        return JsonScanContinue;
    }
    // numbers
    return scanner.genError("in \\u hexadecimal character escape", c);
}

// stateInStringEscU12 is the state after reading `"\u12` during a quoted string.
JsonScanAction stateInStringEscU12(JsonScanner & scanner, char c)
{
    if (('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'))
    {
        scanner.stepFunc = stateInStringEscU123;
        return JsonScanContinue;
    }
    // numbers
    return scanner.genError("in \\u hexadecimal character escape", c);
}

// stateInStringEscU123 is the state after reading `"\u123` during a quoted string.
JsonScanAction stateInStringEscU123(JsonScanner & scanner, char c)
{
    if (('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'))
    {
        scanner.stepFunc = stateInString;
        return JsonScanContinue;
    }
    // numbers
    return scanner.genError("in \\u hexadecimal character escape", c);
}

// stateNeg is the state after reading `-` during a number.
JsonScanAction stateNeg(JsonScanner & scanner, char c)
{
    if (c == '0')
    {
        scanner.stepFunc = state0;
        return JsonScanContinue;
    }
    if ('1' <= c && c <= '9')
    {
        scanner.stepFunc = state1;
        return JsonScanContinue;
    }
    return scanner.genError("in numeric literal", c);
}

// state1 is the state after reading a non-zero integer during a number,
// such as after reading `1` or `100` but not `0`.
JsonScanAction state1(JsonScanner & scanner, char c)
{
    if ('0' <= c && c <= '9')
    {
        scanner.stepFunc = state1;
        return JsonScanContinue;
    }
    return state0(scanner, c);
}

// state0 is the state after reading `0` during a number.
JsonScanAction state0(JsonScanner & scanner, char c)
{
    if (c == '.')
    {
        scanner.stepFunc = stateDot;
        return JsonScanContinue;
    }
    if (c == 'e' || c == 'E')
    {
        scanner.stepFunc = stateE;
        return JsonScanContinue;
    }
    return stateEndValue(scanner, c);
}

// stateDot is the state after reading the integer and decimal point in a number,
// such as after reading `1.`.
JsonScanAction stateDot(JsonScanner & scanner, char c)
{
    if ('0' <= c && c <= '9')
    {
        scanner.stepFunc = stateDot0;
        return JsonScanContinue;
    }
    return scanner.genError("after decimal point in numeric literal", c);
}

// stateDot0 is the state after reading the integer, decimal point, and subsequent
// digits of a number, such as after reading `3.14`.
JsonScanAction stateDot0(JsonScanner & scanner, char c)
{
    if ('0' <= c && c <= '9')
    {
        return JsonScanContinue;
    }
    if (c == 'e' || c == 'E')
    {
        scanner.stepFunc = stateE;
        return JsonScanContinue;
    }
    return stateEndValue(scanner, c);
}

// stateE is the state after reading the mantissa and e in a number,
// such as after reading `314e` or `0.314e`.
JsonScanAction stateE(JsonScanner & scanner, char c)
{
    if (c == '+' || c == '-')
    {
        scanner.stepFunc = stateESign;
        return JsonScanContinue;
    }
    return stateESign(scanner, c);
}

// stateESign is the state after reading the mantissa, e, and sign in a number,
// such as after reading `314e-` or `0.314e+`.
JsonScanAction stateESign(JsonScanner & scanner, char c)
{
    if ('0' <= c && c <= '9')
    {
        scanner.stepFunc = stateE0;
        return JsonScanContinue;
    }
    return scanner.genError("in exponent of numeric literal", c);
}

// stateE0 is the state after reading the mantissa, e, optional sign,
// and at least one digit of the exponent in a number,
// such as after reading `314e-2` or `0.314e+1` or `3.14e0`.
JsonScanAction stateE0(JsonScanner & scanner, char c)
{
    if ('0' <= c && c <= '9')
    {
        return JsonScanContinue;
    }
    return stateEndValue(scanner, c);
}

// stateT is the state after reading `t`.
JsonScanAction stateT(JsonScanner & scanner, char c)
{
    if (c == 'r')
    {
        scanner.stepFunc = stateTr;
        return JsonScanContinue;
    }
    return scanner.genError("in literal true (expecting 'r')", c);
}

// stateTr is the state after reading `tr`.
JsonScanAction stateTr(JsonScanner & scanner, char c)
{
    if (c == 'u')
    {
        scanner.stepFunc = stateTru;
        return JsonScanContinue;
    }
    return scanner.genError("in literal true (expecting 'u')", c);
}

// stateTru is the state after reading `tru`.
JsonScanAction stateTru(JsonScanner & scanner, char c)
{
    if (c == 'e')
    {
        scanner.stepFunc = stateEndValue;
        return JsonScanContinue;
    }
    return scanner.genError("in literal true (expecting 'e')", c);
}

// stateF is the state after reading `f`.
JsonScanAction stateF(JsonScanner & scanner, char c)
{
    if (c == 'a')
    {
        scanner.stepFunc = stateFa;
        return JsonScanContinue;
    }
    return scanner.genError("in literal false (expecting 'a')", c);
}

// stateFa is the state after reading `fa`.
JsonScanAction stateFa(JsonScanner & scanner, char c)
{
    if (c == 'l')
    {
        scanner.stepFunc = stateFal;
        return JsonScanContinue;
    }
    return scanner.genError("in literal false (expecting 'l')", c);
}

// stateFal is the state after reading `fal`.
JsonScanAction stateFal(JsonScanner & scanner, char c)
{
    if (c == 's')
    {
        scanner.stepFunc = stateFals;
        return JsonScanContinue;
    }
    return scanner.genError("in literal false (expecting 's')", c);
}

// stateFals is the state after reading `fals`.
JsonScanAction stateFals(JsonScanner & scanner, char c)
{
    if (c == 'e')
    {
        scanner.stepFunc = stateEndValue;
        return JsonScanContinue;
    }
    return scanner.genError("in literal false (expecting 'e')", c);
}

// stateN is the state after reading `n`.
JsonScanAction stateN(JsonScanner & scanner, char c)
{
    if (c == 'u')
    {
        scanner.stepFunc = stateNu;
        return JsonScanContinue;
    }
    return scanner.genError("in literal null (expecting 'u')", c);
}

// stateNu is the state after reading `nu`.
JsonScanAction stateNu(JsonScanner & scanner, char c)
{
    if (c == 'l')
    {
        scanner.stepFunc = stateNul;
        return JsonScanContinue;
    }
    return scanner.genError("in literal null (expecting 'l')", c);
}

// stateNul is the state after reading `nul`.
JsonScanAction stateNul(JsonScanner & scanner, char c)
{
    if (c == 'l')
    {
        scanner.stepFunc = stateEndValue;
        return JsonScanContinue;
    }
    return scanner.genError("in literal null (expecting 'l')", c);
}

// stateError is the state after reaching a syntax error,
// such as after reading `[1}` or `5.1.2`.
JsonScanAction stateError(JsonScanner &, char)
{
    return JsonScanEnd;
}
} // namespace DB