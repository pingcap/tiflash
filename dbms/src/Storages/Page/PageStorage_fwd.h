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

#include <Storages/Page/ExternalPageCallbacks.h>
#include <Storages/Page/PageDefinesBase.h>

#include <memory>

namespace DB
{

class WriteBatch;
class WriteBatchWrapper;
class UniversalWriteBatch;

class PageStorage;
using PageStoragePtr = std::shared_ptr<PageStorage>;

class PageStorageSnapshot;
using PageStorageSnapshotPtr = std::shared_ptr<PageStorageSnapshot>;

class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

class PageReader;
using PageReaderPtr = std::shared_ptr<PageReader>;

class PageWriter;
using PageWriterPtr = std::shared_ptr<PageWriter>;

} // namespace DB
