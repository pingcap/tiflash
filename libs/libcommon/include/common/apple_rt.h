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
/** Allows to build on MacOS X
  *
  * Highly experimental, not recommended, disabled by default.
  *
  * To use, include this file with -include compiler parameter.
  */

#ifdef __APPLE__

#include <common/config_common.h>

#if APPLE_SIERRA_OR_NEWER == 0
/**
 * MacOS X doesn't support different clock sources
 *
 * Mapping all of them to 0, except for
 * CLOCK_THREAD_CPUTIME_ID, because there is a way
 * to implement it using in-kernel stats about threads
 */
#define CLOCK_MONOTONIC_COARSE 0
#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0
#define CLOCK_THREAD_CPUTIME_ID 3

typedef int clockid_t;
int clock_gettime(int clk_id, struct timespec * t);
#else
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

#endif
