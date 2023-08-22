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

#include <Storages/Page/PageDefinesBase.h>

#include <vector>

namespace DB
{
template <typename Prefix>
struct ExternalPageCallbacksT
{
    // `scanner` for scanning available external page ids on disks.
    // `remover` will be called with living normal page ids after gc run a round, user should remove those
    //           external pages(files) in `pending_external_pages` but not in `valid_normal_pages`
    using PathAndIdsVec = std::vector<std::pair<String, std::set<PageIdU64>>>;
    using ExternalPagesScanner = std::function<PathAndIdsVec()>;
    using ExternalPagesRemover = std::function<
        void(const PathAndIdsVec & pending_external_pages, const std::set<PageIdU64> & valid_normal_pages)>;
    ExternalPagesScanner scanner = nullptr;
    ExternalPagesRemover remover = nullptr;
    Prefix prefix{};
};

using ExternalPageCallbacks = ExternalPageCallbacksT<NamespaceID>;
using UniversalExternalPageCallbacks = ExternalPageCallbacksT<String>;
} // namespace DB
