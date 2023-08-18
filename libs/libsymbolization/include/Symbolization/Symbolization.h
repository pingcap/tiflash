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
#include <cstddef>
#ifdef __cplusplus
extern "C" {
#endif

struct SymbolInfo
{
    const char * symbol_name;
    const char * object_name;
    const char * source_filename;
    size_t source_filename_length;
    size_t lineno;
    size_t svma;
};

SymbolInfo _tiflash_symbolize(void * avma);

#ifdef __cplusplus
}
#endif