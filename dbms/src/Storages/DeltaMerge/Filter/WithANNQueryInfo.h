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

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/VectorIndex_fwd.h>

namespace DB::DM
{

// HACK: We reused existing RSOperator path to pass ANNQueryInfo.
// This is for minimizing changed files in the Serverless.
// When we port back the implementation to open-source version, we should extract the ANNQueryInfo out.
class WithANNQueryInfo : public RSOperator
{
public:
    const RSOperatorPtr child;
    const ANNQueryInfoPtr ann_query_info;

    explicit WithANNQueryInfo(const RSOperatorPtr & child_, const ANNQueryInfoPtr & ann_query_info_)
        : child(child_)
        , ann_query_info(ann_query_info_)
    {}

    String name() override { return "ann"; }

    String toDebugString() override
    {
        if (child)
            return child->toDebugString();
        else
            return "<empty_rs>";
    }

    ColIds getColumnIDs() override
    {
        if (child)
            return child->getColumnIDs();
        else
            return {};
    }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        if (child)
            return child->roughCheck(start_pack, pack_count, param);
        else
            return RSResults(pack_count, RSResult::Some);
    }
};

} // namespace DB::DM
