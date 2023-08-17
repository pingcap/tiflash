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

// https://github.com/pocoproject/poco/blob/devel/Foundation/testsuite/src/TestChannel.h

#pragma once

#include <list>

#include "Poco/Channel.h"
#include "Poco/Message.h"


class TestChannel : public Poco::Channel
{
public:
    using MsgList = std::list<Poco::Message>;

    TestChannel() = default;
    ~TestChannel() override = default;

    void log(const Poco::Message & msg) override
    {
        _msgList.push_back(msg);
        _lastMessage = msg;
    }

    MsgList & list() { return _msgList; }

    void clear() { _msgList.clear(); }

    const Poco::Message & getLastMessage() const { return _lastMessage; }

private:
    MsgList _msgList;
    Poco::Message _lastMessage;
};
