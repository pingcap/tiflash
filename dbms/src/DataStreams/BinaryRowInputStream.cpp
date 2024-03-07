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

#include <Core/Block.h>
#include <DataStreams/BinaryRowInputStream.h>
#include <IO/Buffer/ReadBuffer.h>


namespace DB
{

BinaryRowInputStream::BinaryRowInputStream(ReadBuffer & istr_, const Block & header_)
    : istr(istr_)
    , header(header_)
{}


bool BinaryRowInputStream::read(MutableColumns & columns)
{
    if (istr.eof())
        return false;

    size_t num_columns = columns.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.getByPosition(i).type->deserializeBinary(*columns[i], istr);

    return true;
}

} // namespace DB
