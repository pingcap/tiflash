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
#include <common/aba.h>
#include <common/detect_features.h>

#include <boost/fiber/detail/cpu_relax.hpp>
void common::aba_generic_cpu_relax()
{
    cpu_relax();
}

bool common::aba_runtime_has_xchg16b()
{
#if defined(__aarch64__)
    return cpu_feature_flags.atomics;
#elif defined(__x86_64__)
    return cpu_feature_flags.cx16;
#else
    return false;
#endif
}

bool common::aba_xchg16b(std::atomic<common::Linked> & src,
                         common::Linked & cmp,
                         common::Linked with)
{
    return src.compare_exchange_weak(
        cmp,
        with,
        std::memory_order_acq_rel,
        std::memory_order_relaxed);
}
