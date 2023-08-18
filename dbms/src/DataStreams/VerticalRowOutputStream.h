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

#include <Core/Block.h>
#include <Core/Names.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Stream to output data in format "each value in separate row".
  * Usable to show few rows with many columns.
  */
class VerticalRowOutputStream : public IRowOutputStream
{
public:
    VerticalRowOutputStream(WriteBuffer & ostr_, const Block & sample_, size_t max_rows_);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeRowStartDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writeSuffix() override;

    void flush() override;

    void setExtremes(const Block & extremes_) override { extremes = extremes_; }

protected:
    virtual void writeValue(const IColumn & column, const IDataType & type, size_t row_num) const;

    void writeExtremes();
    /// For totals and extremes.
    void writeSpecialRow(const Block & block, size_t row_num, const char * title);

    WriteBuffer & ostr;
    const Block sample;
    size_t max_rows;
    size_t field_number = 0;
    size_t row_number = 0;

    using NamesAndPaddings = std::vector<String>;
    NamesAndPaddings names_and_paddings;

    Block extremes;
};


/** Same but values are printed without escaping.
  */
class VerticalRawRowOutputStream final : public VerticalRowOutputStream
{
public:
    using VerticalRowOutputStream::VerticalRowOutputStream;

protected:
    void writeValue(const IColumn & column, const IDataType & type, size_t row_num) const override;
};

} // namespace DB
