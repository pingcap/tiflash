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

#include <Flash/Coprocessor/CoprocessorReader.h>

namespace DB
{
class CoprocessorReader;
using CoprocessorReaderPtr = std::shared_ptr<CoprocessorReader>;
class ExchangeReceiver;
using ExchangeReceiverPtr = std::shared_ptr<ExchangeReceiver>;
using ExchangeReceiverMap = std::unordered_map<String, ExchangeReceiverPtr>;

class MPPReceiverSet
{
public:
    explicit MPPReceiverSet(const String & req_id)
        : log(Logger::get(req_id))
    {}
    ~MPPReceiverSet()
    {
        /// close will close every receiver's internal MPMC queue and avoid blocking risk in waitAllConnectionsDone
        close();
    }
    void addExchangeReceiver(const String & executor_id, const ExchangeReceiverPtr & exchange_receiver);
    void addCoprocessorReader(const CoprocessorReaderPtr & coprocessor_reader);
    ExchangeReceiverPtr getExchangeReceiver(const String & executor_id) const;
    void cancel();
    void close();

    int getExternalThreadCnt() const { return external_thread_cnt; }

private:
    /// two kinds of receiver in MPP
    /// ExchangeReceiver: receiver data from other MPPTask
    /// CoprocessorReader: used in remote read
    ExchangeReceiverMap exchange_receiver_map;
    std::vector<CoprocessorReaderPtr> coprocessor_readers;
    const LoggerPtr log;
    int external_thread_cnt = 0;
};

using MPPReceiverSetPtr = std::shared_ptr<MPPReceiverSet>;

} // namespace DB
