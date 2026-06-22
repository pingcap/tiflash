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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Storages/Columnar/ColumnarReader.h>
#include <Storages/Columnar/ColumnarStreams.h>

// This header exists as an umbrella include for the Columnar storage types.
// The actual type definitions have been moved to:
//   - Storages/Columnar/ColumnarReader.h  (RNColumnarReader*, createColumnarReader, etc.)
//   - Storages/Columnar/ColumnarStreams.h (RNColumnarInputStream, RNColumnarSourceOp)
#endif
