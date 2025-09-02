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

namespace DB::DM
{

enum class ReadMode
{
    /**
     * Read in normal mode. Data is ordered by PK, and only the most recent version is returned.
     */
    Normal,

    /**
     * Read in fast mode. Data is not sort merged, and all versions are returned. However, deleted records (del_mark=1)
     * will be still filtered out.
     */
    Fast,

    /**
     * Read in raw mode, for example, for statements like `SELRAW *`. In raw mode, data is not sort merged and all versions
     * are just returned.
     */
    Raw,

    Bitmap,
};

enum class ReadTag
{
    Internal, // Read columns required by some internal tasks.
    Query, // Read columns required by queries.
    MVCC, // Read columns to build MVCC bitmap.
    LMFilter, // Read columns required by late-materialization filter.
};
} // namespace DB::DM
