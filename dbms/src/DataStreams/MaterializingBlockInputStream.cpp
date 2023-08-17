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

#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/materializeBlock.h>


namespace DB
{

MaterializingBlockInputStream::MaterializingBlockInputStream(const BlockInputStreamPtr & input)
{
    children.push_back(input);
}

String MaterializingBlockInputStream::getName() const
{
    return "Materializing";
}

Block MaterializingBlockInputStream::getHeader() const
{
    return materializeBlock(children.back()->getHeader());
}

Block MaterializingBlockInputStream::readImpl()
{
    return materializeBlock(children.back()->read());
}

} // namespace DB
