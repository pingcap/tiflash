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

#include <DataStreams/IBlockInputStream.h>

#include <vector>

namespace DB
{

class IDictionarySource;
using DictionarySourcePtr = std::unique_ptr<IDictionarySource>;

/** Data-provider interface for external dictionaries,
*    abstracts out the data source (file, MySQL, ClickHouse, external program, network request et cetera)
*    from the presentation and memory layout (the dictionary itself).
*/
class IDictionarySource
{
public:
    /// Returns an input stream with all the data available from this source.
    virtual BlockInputStreamPtr loadAll() = 0;

    /// Returns an input stream with updated data available from this source.
    virtual BlockInputStreamPtr loadUpdatedAll() = 0;

    /** Indicates whether this source supports "random access" loading of data
      *  loadId and loadIds can only be used if this function returns true.
      */
    virtual bool supportsSelectiveLoad() const = 0;

    /** Returns an input stream with the data for a collection of identifiers.
      * It must be guaranteed, that 'ids' array will live at least until all data will be read from returned stream.
      */
    virtual BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) = 0;

    /** Returns an input stream with the data for a collection of composite keys.
      * `requested_rows` contains indices of all rows containing unique keys.
      * It must be guaranteed, that 'requested_rows' array will live at least until all data will be read from returned stream.
      */
    virtual BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) = 0;

    /// indicates whether the source has been modified since last load* operation
    virtual bool isModified() const = 0;

    /// Returns true if update field is defined
    virtual bool hasUpdateField() const = 0;

    virtual DictionarySourcePtr clone() const = 0;

    /// returns an informal string describing the source
    virtual std::string toString() const = 0;

    virtual ~IDictionarySource() = default;
};

} // namespace DB
