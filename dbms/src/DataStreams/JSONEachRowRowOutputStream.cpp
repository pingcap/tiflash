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

#include <DataStreams/JSONEachRowRowOutputStream.h>
#include <IO/Buffer/WriteBufferValidUTF8.h>
#include <IO/WriteHelpers.h>


namespace DB
{


JSONEachRowRowOutputStream::JSONEachRowRowOutputStream(
    WriteBuffer & ostr_,
    const Block & sample,
    const FormatSettingsJSON & settings_)
    : ostr(ostr_)
    , settings(settings_)
{
    size_t columns = sample.columns();
    fields.resize(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString out(fields[i]);
        writeJSONString(sample.getByPosition(i).name, out);
    }
}


void JSONEachRowRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeString(fields[field_number], ostr);
    writeChar(':', ostr);
    type.serializeTextJSON(column, row_num, ostr, settings);
    ++field_number;
}


void JSONEachRowRowOutputStream::writeFieldDelimiter()
{
    writeChar(',', ostr);
}


void JSONEachRowRowOutputStream::writeRowStartDelimiter()
{
    writeChar('{', ostr);
}


void JSONEachRowRowOutputStream::writeRowEndDelimiter()
{
    writeCString("}\n", ostr);
    field_number = 0;
}

} // namespace DB
