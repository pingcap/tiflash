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

#include <setjmp.h>

int main()
{
    sigjmp_buf env;
    int val;
    volatile int count = 0;
    val = sigsetjmp(env, 0);
    ++count;
    if (count == 1 && val != 0)
    {
        return 1;
    }
    if (count == 2 && val == 42)
    {
        return 0;
    }
    if (count == 1)
    {
        siglongjmp(env, 42);
    }
    return 1;
}
