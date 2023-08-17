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

#include <DataStreams/MockExchangeSenderInputStream.h>
namespace DB
{
MockExchangeSenderInputStream::MockExchangeSenderInputStream(const BlockInputStreamPtr & input, const String & req_id)
    : log(Logger::get(req_id))
{
    children.push_back(input);
}

Block MockExchangeSenderInputStream::getHeader() const
{
    return children.back()->getHeader();
}

Block MockExchangeSenderInputStream::readImpl()
{
    return children.back()->read();
}

} // namespace DB
