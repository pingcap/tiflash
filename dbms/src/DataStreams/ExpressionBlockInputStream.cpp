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

#include <DataStreams/ExpressionBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
template <bool selective>
ExpressionBlockInputStream<selective>::ExpressionBlockInputStream(
    const BlockInputStreamPtr & input,
    const ExpressionActionsPtr & expression_,
    const String & req_id)
    : expression(expression_)
    , log(Logger::get(req_id))
{
    children.push_back(input);
}

template <bool selective>
Block ExpressionBlockInputStream<selective>::getHeader() const
{
    Block res = children.back()->getHeader();
    expression->execute(res);
    return res;
}

template <bool selective>
Block ExpressionBlockInputStream<selective>::readImpl()
{
    Block res = children.back()->read();
    if (!res)
        return res;

    if constexpr (selective)
    {
        auto ori_info = res.info;
        expression->execute(res);
        res.info = ori_info;
    }
    else
    {
        expression->execute(res);
    }
    return res;
}

template class ExpressionBlockInputStream<true>;
template class ExpressionBlockInputStream<false>;
} // namespace DB
