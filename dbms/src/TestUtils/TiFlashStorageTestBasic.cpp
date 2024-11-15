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

#include <Interpreters/Settings.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

namespace DB::base
{
void TiFlashStorageTestBasic::reload()
{
    reload({});
}

void TiFlashStorageTestBasic::reload(const DB::Settings & db_settings)
{
    Strings test_paths;
    test_paths.push_back(getTemporaryPath());
    db_context = DB::tests::TiFlashTestEnv::getContext(db_settings, test_paths);
}

} // namespace DB::base
