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
        : RSOperator({child_})
        , child(child_)
        , ann_query_info(ann_query_info_)
    {}

    String name() override { return "ann"; }

    Attrs getAttrs() override
    {
        if (children[0])
            return children[0]->getAttrs();
        else
            return {};
    }

    String toDebugString() override
    {
        if (children[0])
            return children[0]->toDebugString();
        else
            return "<empty_rs>";
    }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        if (children[0])
            return children[0]->roughCheck(start_pack, pack_count, param);
        else
            return RSResults(pack_count, RSResult::Unknown);
    }
};

} // namespace DB::DM
