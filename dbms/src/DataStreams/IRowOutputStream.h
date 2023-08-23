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

#include <boost/noncopyable.hpp>
#include <cstdint>
#include <memory>


namespace DB
{

class Block;
class IColumn;
class IDataType;
struct Progress;


/** Interface of stream for writing data by rows (for example: for output to terminal).
  */
class IRowOutputStream : private boost::noncopyable
{
public:
    /** Write a row.
      * Default implementation calls methods to write single values and delimiters
      * (except delimiter between rows (writeRowBetweenDelimiter())).
      */
    virtual void write(const Block & block, size_t row_num);

    /** Write single value. */
    virtual void writeField(const IColumn & column, const IDataType & type, size_t row_num) = 0;

    /** Write delimiter. */
    virtual void writeFieldDelimiter(){}; /// delimiter between values
    virtual void writeRowStartDelimiter(){}; /// delimiter before each row
    virtual void writeRowEndDelimiter(){}; /// delimiter after each row
    virtual void writeRowBetweenDelimiter(){}; /// delimiter between rows
    virtual void writePrefix(){}; /// delimiter before resultset
    virtual void writeSuffix(){}; /// delimiter after resultset

    /** Flush output buffers if any. */
    virtual void flush() {}

    /** Methods to set additional information for output in formats, that support it.
      */
    virtual void setRowsBeforeLimit(size_t /*rows_before_limit*/) {}
    virtual void setExtremes(const Block & /*extremes*/) {}

    /** Notify about progress. Method could be called from different threads.
      * Passed value are delta, that must be summarized.
      */
    virtual void onProgress(const Progress & /*progress*/) {}

    /** Content-Type to set when sending HTTP response. */
    virtual String getContentType() const { return "text/plain; charset=UTF-8"; }

    virtual ~IRowOutputStream() {}
};

using RowOutputStreamPtr = std::shared_ptr<IRowOutputStream>;

} // namespace DB
