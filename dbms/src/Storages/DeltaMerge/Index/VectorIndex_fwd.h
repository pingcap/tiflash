// Copyright 2024 PingCAP, Inc.
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

#include <tipb/executor.pb.h>

namespace DB::DM
{

using ANNQueryInfoPtr = std::shared_ptr<tipb::ANNQueryInfo>;

class VectorIndexBuilder;
using VectorIndexBuilderPtr = std::shared_ptr<VectorIndexBuilder>;

class VectorIndexViewer;
using VectorIndexViewerPtr = std::shared_ptr<VectorIndexViewer>;

class VectorIndexCache;
using VectorIndexCachePtr = std::shared_ptr<VectorIndexCache>;

} // namespace DB::DM
