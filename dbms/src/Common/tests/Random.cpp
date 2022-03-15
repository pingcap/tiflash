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

/// Taken from SMHasher.
#ifdef __APPLE__
#include <common/apple_rt.h>
#endif
#include "Random.h"

Rand g_rand1(1);
Rand g_rand2(2);
Rand g_rand3(3);
Rand g_rand4(4);

//-----------------------------------------------------------------------------
