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

#include <Storages/Transaction/Collator.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

namespace DB::ExchangeSenderInterpreterHelper
{
std::vector<Int64> genPartitionColIds(const tipb::ExchangeSender & exchange_sender);

TiDB::TiDBCollators genPartitionColCollators(const tipb::ExchangeSender & exchange_sender);
} // namespace DB::ExchangeSenderInterpreterHelper