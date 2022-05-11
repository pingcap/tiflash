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

#define DISALLOW_COPY(ClassName)           \
    ClassName(const ClassName &) = delete; \
    ClassName & operator=(const ClassName &) = delete

#define DISALLOW_MOVE(ClassName)      \
    ClassName(ClassName &&) = delete; \
    ClassName & operator=(ClassName &&) = delete

#define DISALLOW_COPY_AND_MOVE(ClassName) \
    DISALLOW_COPY(ClassName);             \
    DISALLOW_MOVE(ClassName)
