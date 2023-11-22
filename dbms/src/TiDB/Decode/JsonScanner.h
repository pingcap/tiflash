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
#include <fmt/format.h>

#include <stack>

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

struct JsonSyntaxError
{
    String msg;
    Int64 offset;
};

struct JsonScanner;

// This limits the max nesting depth to prevent stack overflow.
// This is permitted by https://tools.ietf.org/html/rfc7159#section-9
const static Int64 kJsonMaxNestingDepth = 10000;

JsonScanAction stateBeginValueOrEmpty(JsonScanner & scanner, char c);
JsonScanAction stateBeginValue(JsonScanner & scanner, char c);
JsonScanAction stateBeginStringOrEmpty(JsonScanner & scanner, char c);
JsonScanAction stateBeginString(JsonScanner & scanner, char c);
JsonScanAction stateEndValue(JsonScanner & scanner, char c);
JsonScanAction stateEndTop(JsonScanner & scanner, char c);
JsonScanAction stateInString(JsonScanner & scanner, char c);
JsonScanAction stateInStringEsc(JsonScanner & scanner, char c);
JsonScanAction stateInStringEscU(JsonScanner & scanner, char c);
JsonScanAction stateInStringEscU1(JsonScanner & scanner, char c);
JsonScanAction stateInStringEscU12(JsonScanner & scanner, char c);
JsonScanAction stateInStringEscU123(JsonScanner & scanner, char c);
JsonScanAction stateNeg(JsonScanner & scanner, char c);
JsonScanAction state1(JsonScanner & scanner, char c);
JsonScanAction state0(JsonScanner & scanner, char c);
JsonScanAction stateDot(JsonScanner & scanner, char c);
JsonScanAction stateDot0(JsonScanner & scanner, char c);
JsonScanAction stateE(JsonScanner & scanner, char c);
JsonScanAction stateESign(JsonScanner & scanner, char c);
JsonScanAction stateE0(JsonScanner & scanner, char c);
JsonScanAction stateT(JsonScanner & scanner, char c);
JsonScanAction stateTr(JsonScanner & scanner, char c);
JsonScanAction stateTru(JsonScanner & scanner, char c);
JsonScanAction stateF(JsonScanner & scanner, char c);
JsonScanAction stateFa(JsonScanner & scanner, char c);
JsonScanAction stateFal(JsonScanner & scanner, char c);
JsonScanAction stateFals(JsonScanner & scanner, char c);
JsonScanAction stateN(JsonScanner & scanner, char c);
JsonScanAction stateNu(JsonScanner & scanner, char c);
JsonScanAction stateNul(JsonScanner & scanner, char c);
JsonScanAction stateError(JsonScanner & scanner, char c);

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
struct JsonScanner
{
    // error records an error and switches to the error state.
    JsonScanAction genError(String msg, char c)
    {
        stepFunc = &stateError;
        error.msg = fmt::format("invalid character {} {}", c, msg);
        error.offset = bytes;
        return JsonScanError;
    }

    // eof tells the scanner that the end of input has been reached.
    // It returns a scan status just as s.step does.
    JsonScanAction eof()
    {
        if (!error.msg.empty())
        {
            return JsonScanError;
        }
        if (end_top)
        {
            return JsonScanEnd;
        }
        stepFunc(*this, ' ');
        if (end_top)
        {
            return JsonScanEnd;
        }
        if (!error.msg.empty())
        {
            error.msg = "unexpected end of JSON input";
            error.offset = bytes;
        }
        return JsonScanError;
    }

    // pushParseState pushes a new parse state p onto the parse stack.
    // an error state is returned if maxNestingDepth was exceeded, otherwise successState is returned.
    JsonScanAction pushParseState(char c, JsonScanState new_parse_state, JsonScanAction success_action)
    {
        parse_state_stack.push(new_parse_state);
        if (parse_state_stack.size() <= kJsonMaxNestingDepth)
        {
            return success_action;
        }
        return genError("exceeded max depth", c);
    }

    // popParseState pops a parse state (already obtained) off the stack
    // and updates s.step accordingly.
    void popParseState()
    {
        parse_state_stack.pop();
        if (parse_state_stack.empty())
        {
            stepFunc = &stateEndTop;
            end_top = true;
        }
        else
        {
            stepFunc = stateEndValue;
        }
    }

    // The step is a func to be called to execute the next transition.
    // Also tried using an integer constant and a single func
    // with a switch, but using the func directly was 10% faster
    // on a 64-bit Mac Mini, and it's nicer to read.
    JsonScanAction (*stepFunc)(JsonScanner &, char) = stateBeginValue;

    // Reached end of top-level value.
    bool end_top = false;
    // Stack of what we're in the middle of - array values, object keys, object values.
    std::stack<JsonScanState> parse_state_stack;
    // Error that happened, if any.
    JsonSyntaxError error;
    // total bytes consumed, updated by decoder.Decode (and deliberately
    // not set to zero by scan.reset)
    Int64 bytes = 0;
};

// checkJsonValid verifies that data is valid JSON-encoded data.
bool checkJsonValid(const char * data, size_t length);
} // namespace DB