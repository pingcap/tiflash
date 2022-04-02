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

#include <Storages/Transaction/ProxyFFI.h>

namespace DB
{

struct SSTReader
{
    bool remained() const;
    BaseBuffView key() const;
    BaseBuffView value() const;
    void next();

    SSTReader(const SSTReader &) = delete;
    SSTReader(SSTReader &&) = delete;
    SSTReader(const TiFlashRaftProxyHelper * proxy_helper_, SSTView view);
    ~SSTReader();

private:
    const TiFlashRaftProxyHelper * proxy_helper;
    SSTReaderPtr inner;
    ColumnFamilyType type;
};


} // namespace DB
