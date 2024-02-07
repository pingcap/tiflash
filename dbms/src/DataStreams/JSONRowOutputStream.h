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

#include <Common/Stopwatch.h>
#include <Core/Block.h>
#include <DataStreams/IRowOutputStream.h>
#include <DataTypes/FormatSettingsJSON.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/Progress.h>

namespace DB
{

/** Stream for output data in JSON format.
  */
class JSONRowOutputStream : public IRowOutputStream
{
public:
    JSONRowOutputStream(
        WriteBuffer & ostr_,
        const Block & sample_,
        bool write_statistics_,
        const FormatSettingsJSON & settings_);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void flush() override
    {
        ostr->next();

        if (validating_ostr)
            dst_ostr.next();
    }

    void setRowsBeforeLimit(size_t rows_before_limit_) override
    {
        applied_limit = true;
        rows_before_limit = rows_before_limit_;
    }

    void setExtremes(const Block & extremes_) override { extremes = extremes_; }

    void onProgress(const Progress & value) override;

    String getContentType() const override { return "application/json; charset=UTF-8"; }

protected:
    void writeRowsBeforeLimitAtLeast();
    virtual void writeExtremes();
    void writeStatistics();

    WriteBuffer & dst_ostr;
    std::unique_ptr<WriteBuffer>
        validating_ostr; /// Validates UTF-8 sequences, replaces bad sequences with replacement character.
    WriteBuffer * ostr;

    size_t field_number = 0;
    size_t row_count = 0;
    bool applied_limit = false;
    size_t rows_before_limit = 0;
    NamesAndTypes fields;
    Block extremes;

    Progress progress;
    Stopwatch watch;
    bool write_statistics;
    FormatSettingsJSON settings;
};

} // namespace DB
