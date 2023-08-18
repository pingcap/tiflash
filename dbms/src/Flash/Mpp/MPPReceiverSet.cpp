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

#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Mpp/MPPReceiverSet.h>

namespace DB
{
void MPPReceiverSet::addExchangeReceiver(const String & executor_id, const ExchangeReceiverPtr & exchange_receiver)
{
    RUNTIME_ASSERT(
        exchange_receiver_map.find(executor_id) == exchange_receiver_map.end(),
        log,
        "Duplicate executor_id: {} in DAGRequest",
        executor_id);
    exchange_receiver_map[executor_id] = exchange_receiver;
    external_thread_cnt += exchange_receiver->getExternalThreadCnt();
}

void MPPReceiverSet::addCoprocessorReader(const CoprocessorReaderPtr & coprocessor_reader)
{
    coprocessor_readers.push_back(coprocessor_reader);
    external_thread_cnt += coprocessor_reader->getConcurrency();
}

ExchangeReceiverPtr MPPReceiverSet::getExchangeReceiver(const String & executor_id) const
{
    auto it = exchange_receiver_map.find(executor_id);
    if (unlikely(it == exchange_receiver_map.end()))
        return nullptr;
    return it->second;
}

void MPPReceiverSet::cancel()
{
    for (auto & it : exchange_receiver_map)
        it.second->cancel();
    for (auto & cop_reader : coprocessor_readers)
        cop_reader->cancel();
}

void MPPReceiverSet::close()
{
    for (auto & it : exchange_receiver_map)
        it.second->close();
    for (auto & cop_reader : coprocessor_readers)
        cop_reader->close();
}

} // namespace DB
