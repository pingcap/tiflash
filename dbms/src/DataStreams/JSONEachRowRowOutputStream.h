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
#include <IO/WriteBuffer.h>
#include <DataStreams/IRowOutputStream.h>
#include <DataTypes/FormatSettingsJSON.h>


namespace DB
{

/** The stream for outputting data in JSON format, by object per line.
  * Does not validate UTF-8.
  */
class JSONEachRowRowOutputStream : public IRowOutputStream
{
public:
    JSONEachRowRowOutputStream(WriteBuffer & ostr_, const Block & sample, const FormatSettingsJSON & settings);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

    void flush() override
    {
        ostr.next();
    }

private:
    WriteBuffer & ostr;
    size_t field_number = 0;
    Names fields;

    FormatSettingsJSON settings;
};

}

