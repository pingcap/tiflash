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

#include <Common/HashTable/HashMap.h>
#include <Core/Block.h>
#include <DataStreams/IRowInputStream.h>


namespace DB
{

class ReadBuffer;


/** Stream for reading data in TSKV format.
  * TSKV is a very inefficient data format.
  * Similar to TSV, but each field is written as key=value.
  * Fields can be listed in any order (including, in different lines there may be different order),
  *  and some fields may be missing.
  * An equal sign can be escaped in the field name.
  * Also, as an additional element there may be a useless tskv fragment - it needs to be ignored.
  */
class TSKVRowInputStream : public IRowInputStream
{
public:
    TSKVRowInputStream(ReadBuffer & istr_, const Block & header_, bool skip_unknown_);

    bool read(MutableColumns & columns) override;
    bool allowSyncAfterError() const override { return true; };
    void syncAfterError() override;

private:
    ReadBuffer & istr;
    Block header;
    /// Skip unknown fields.
    bool skip_unknown;

    /// Buffer for the read from the stream the field name. Used when you have to copy it.
    String name_buf;

    /// Hash table matching `field name -> position in the block`. NOTE You can use perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;
};

} // namespace DB
