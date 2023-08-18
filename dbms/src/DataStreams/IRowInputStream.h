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

#include <Columns/IColumn.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <string>


namespace DB
{


/** Interface of stream, that allows to read data by rows.
  */
class IRowInputStream : private boost::noncopyable
{
public:
    /** Read next row and append it to the columns.
      * If no more rows - return false.
      */
    virtual bool read(MutableColumns & columns) = 0;

    virtual void readPrefix(){}; /// delimiter before begin of result
    virtual void readSuffix(){}; /// delimiter after end of result

    /// Skip data until next row.
    /// This is intended for text streams, that allow skipping of errors.
    /// By default - throws not implemented exception.
    virtual bool allowSyncAfterError() const { return false; }
    virtual void syncAfterError();

    /// In case of parse error, try to roll back and parse last one or two rows very carefully
    ///  and collect as much as possible diagnostic information about error.
    /// If not implemented, returns empty string.
    virtual std::string getDiagnosticInfo() { return {}; };

    virtual ~IRowInputStream() {}
};

using RowInputStreamPtr = std::shared_ptr<IRowInputStream>;

} // namespace DB
