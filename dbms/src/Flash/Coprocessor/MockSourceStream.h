// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Core/ColumnsWithTypeAndName.h>


namespace DB
{
template <typename SourceType>
std::pair<NamesAndTypes, std::vector<std::shared_ptr<SourceType>>> mockSourceStream(Context & context, size_t max_streams, DB::LoggerPtr log, String executor_id);
} // namespace DB