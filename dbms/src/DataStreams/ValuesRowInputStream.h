// Copyright 2022 PingCAP, Ltd.
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

#include <Core/Block.h>
#include <DataStreams/IRowInputStream.h>


namespace DB
{

class Context;
class ReadBuffer;


/** Stream to read data in VALUES format (as in INSERT query).
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
    /** Data is parsed using fast, streaming parser.
      * If interpret_expressions is true, it will, in addition, try to use SQL parser and interpreter
      *  in case when streaming parser could not parse field (this is very slow).
      */
    ValuesRowInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_, bool interpret_expressions_);

    bool read(MutableColumns & columns) override;

private:
    ReadBuffer & istr;
    Block header;
    const Context & context;
    bool interpret_expressions;
};

}
