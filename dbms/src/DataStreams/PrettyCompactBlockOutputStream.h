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

#include <DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Prints the result in the form of beautiful tables, but with fewer delimiter lines.
  */
class PrettyCompactBlockOutputStream : public PrettyBlockOutputStream
{
public:
    PrettyCompactBlockOutputStream(
        WriteBuffer & ostr_,
        const Block & header_,
        bool no_escapes_,
        size_t max_rows_,
        const Context & context_)
        : PrettyBlockOutputStream(ostr_, header_, no_escapes_, max_rows_, context_)
    {}

    void write(const Block & block) override;

protected:
    void writeHeader(const Block & block, const Widths & max_widths, const Widths & name_widths);
    void writeBottom(const Widths & max_widths);
    void writeRow(size_t row_num, const Block & block, const WidthsPerColumn & widths, const Widths & max_widths);
};

} // namespace DB
