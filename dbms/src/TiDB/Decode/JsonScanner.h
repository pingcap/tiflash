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

#pragma once

#include <Core/Types.h>
#include <common/StringRef.h>
#include <common/memcpy.h>

namespace DB
{

// C++ implementation of https://github.com/golang/go/blob/master/src/encoding/json/scanner.go
// Most of the comments are copied from go-lib directly.

// These values are returned by the state transition functions
// assigned to scanner.state and the method scanner.eof.
// They give details about the current state of the scan that
// callers might be interested to know about.
// It is okay to ignore the return value of any particular
// call to scanner.state: if one call returns scanError,
// every subsequent call will return scanError too.
enum JsonScanAction
{
    JsonScanContinue, // uninteresting byte
    JsonScanBeginLiteral, // end implied by next result != scanContinue
    JsonScanBeginObject, // begin object
    JsonScanObjectKey, // just finished object key (string)
    JsonScanObjectValue, // just finished non-last object value
    JsonScanEndObject, // end object (implies scanObjectValue if possible)
    JsonScanBeginArray, // begin array
    JsonScanArrayValue, // just finished array value
    JsonScanEndArray, // end array (implies scanArrayValue if possible)
    JsonScanSkipSpace, // space byte; can skip; known to be last "continue" result
    // Stop.
    JsonScanEnd, // top-level value ended *before* this byte; known to be first "stop" result
    JsonScanError, // hit an error, scanner.err.
};

// These values are stored in the parseState stack.
// They give the current state of a composite value
// being scanned. If the parser is inside a nested value
// the parseState describes the nested state, outermost at entry 0.
enum JsonScanState
{
    JsonScanParseObjectKey, // parsing object key (before colon)
    JsonScanParseObjectValue, // parsing object value (after colon)
    JsonScanParseArrayValue, // parsing array value
};

struct JsonSyntaxError {
    String msg;
    Int64 offset;
};

// This limits the max nesting depth to prevent stack overflow.
// This is permitted by https://tools.ietf.org/html/rfc7159#section-9
const static Int64 kJsonMaxNestingDepth = 10000;

// A scanner is a JSON scanning state machine.
// Callers call scan.reset and then pass bytes in one at a time
// by calling scan.step(&scan, c) for each byte.
// The return value, referred to as an opcode, tells the
// caller about significant parsing events like beginning
// and ending literals, objects, and arrays, so that the
// caller can follow along if it wishes.
// The return value scanEnd indicates that a single top-level
// JSON value has been completed, *before* the byte that
// just got passed in.  (The indication must be delayed in order
// to recognize the end of numbers: is 123 a whole value or
// the beginning of 12345e+6?).
struct JsonScanner {
    // eof tells the scanner that the end of input has been reached.
    // It returns a scan status just as s.step does.
    JsonScanAction eof() {
        if (!error.msg.empty()) {
            return JsonScanError;
        }
        if (end_top) {
            return JsonScanEnd;
        }
        stepFunc(*this, ' ');
        if (end_top) {
            return JsonScanEnd;
        }
        if (!error.msg.empty()) {
            error.msg = "unexpected end of JSON input";
            error.offset = bytes;
        }
        return JsonScanError;
    }

    // pushParseState pushes a new parse state p onto the parse stack.
    // an error state is returned if maxNestingDepth was exceeded, otherwise successState is returned.
    JsonScanAction pushParseState(char c, JsonScanState new_parse_state, JsonScanAction success_action) {
        parse_state_vec.push_back(new_parse_state);
        if (parse_state_vec.size() <= kJsonMaxNestingDepth) {
            return success_action;
        }
        return error(c, "exceeded max depth");
    }

    // popParseState pops a parse state (already obtained) off the stack
    // and updates s.step accordingly.
    void popParseState() {
        n := len(s.parseState) - 1
                              s.parseState = s.parseState[0:n]
              if n == 0 {
            s.step = stateEndTop
                         s.endTop = true
        } else {
            s.step = stateEndValue
        }
    }

    // The step is a func to be called to execute the next transition.
    // Also tried using an integer constant and a single func
    // with a switch, but using the func directly was 10% faster
    // on a 64-bit Mac Mini, and it's nicer to read.
    JsonScanAction (*stepFunc)(JsonScanner &, char);

    // Reached end of top-level value.
    bool end_top = false;
    // Stack of what we're in the middle of - array values, object keys, object values.
    std::vector<JsonScanState> parse_state_vec;
    // Error that happened, if any.
    JsonSyntaxError error;
    // total bytes consumed, updated by decoder.Decode (and deliberately
    // not set to zero by scan.reset)
    Int64 bytes = 0;
};

// checkJsonValid verifies that data is valid JSON-encoded data.
bool checkJsonValid(const char * data, size_t length) {
    JsonScanner scanner;
    for (size_t i = 0; i < length; ++i)
    {
        ++scanner.bytes;
        if (scanner.stepFunc(scanner, data[i]) == JsonScanError)
        {
            return false;
        }
    }
    if (scanner.eof() == JsonScanError)
    {
        return false;
    }
    return true;
}

bool JsonScanIsSpace(char c) {
    return c <= ' ' && (c == ' ' || c == '\t' || c == '\r' || c == '\n');
}

// stateBeginValueOrEmpty is the state after reading `[`.
JsonScanAction stateBeginValueOrEmpty(JsonScanner & scanner, char c) {
    if (JsonScanIsSpace(c)) {
        return JsonScanSkipSpace;
    }
    if (c == ']') {
        return stateEndValue(scanner, c);
    }
    return stateBeginValue(scanner, c);
}

// stateBeginValue is the state at the beginning of the input.
JsonScanAction stateBeginValue(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

//////////==========================TODO==========================//////////
// stateEndTop is the state after finishing the top-level value,
// such as after reading `{}` or `[1,2,3]`.
// Only space characters should be seen now.
JsonScanAction stateEndTop(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateInString is the state after reading `"`.
JsonScanAction stateInString(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateInStringEsc is the state after reading `"\` during a quoted string.
JsonScanAction stateInStringEsc(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateInStringEscU is the state after reading `"\u` during a quoted string.
JsonScanAction stateInStringEscU(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateInStringEscU1 is the state after reading `"\u1` during a quoted string.
JsonScanAction stateInStringEscU1(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateInStringEscU12 is the state after reading `"\u12` during a quoted string.
JsonScanAction stateInStringEscU12(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateInStringEscU123 is the state after reading `"\u123` during a quoted string.
JsonScanAction stateInStringEscU123(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateNeg is the state after reading `-` during a number.
JsonScanAction stateNeg(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// state1 is the state after reading a non-zero integer during a number,
// such as after reading `1` or `100` but not `0`.
JsonScanAction state1(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// state0 is the state after reading `0` during a number.
JsonScanAction state0(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateDot is the state after reading the integer and decimal point in a number,
// such as after reading `1.`.
JsonScanAction stateDot(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateDot0 is the state after reading the integer, decimal point, and subsequent
// digits of a number, such as after reading `3.14`.
JsonScanAction stateDot0(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateE is the state after reading the mantissa and e in a number,
// such as after reading `314e` or `0.314e`.
JsonScanAction stateE(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateESign is the state after reading the mantissa, e, and sign in a number,
// such as after reading `314e-` or `0.314e+`.
JsonScanAction stateESign(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateE0 is the state after reading the mantissa, e, optional sign,
// and at least one digit of the exponent in a number,
// such as after reading `314e-2` or `0.314e+1` or `3.14e0`.
JsonScanAction stateE0(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateT is the state after reading `t`.
JsonScanAction stateT(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateTr is the state after reading `tr`.
JsonScanAction stateTr(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateTru is the state after reading `tru`.
JsonScanAction stateTru(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateF is the state after reading `f`.
JsonScanAction stateF(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateFa is the state after reading `fa`.
JsonScanAction stateFa(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateFal is the state after reading `fal`.
JsonScanAction stateFal(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateFals is the state after reading `fals`.
JsonScanAction stateFals(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateN is the state after reading `n`.
JsonScanAction stateN(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateNu is the state after reading `nu`.
JsonScanAction stateNu(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateNul is the state after reading `nul`.
JsonScanAction stateNul(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

// stateError is the state after reaching a syntax error,
// such as after reading `[1}` or `5.1.2`.
JsonScanAction stateError(JsonScanner & scanner, char c) {
    return JsonScanEnd;
}

} // namespace DB