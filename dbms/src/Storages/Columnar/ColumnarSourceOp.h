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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Operators/Operator.h>
#include <Operators/SharedQueue.h>

namespace DB
{

/// Queue consumer for columnar pipeline read.
/// ColumnarReadSourceOp owns the actual reader work and pushes deserialized blocks into SharedQueue.
class RNColumnarSourceOp : public SourceOp
{
    static constexpr auto NAME = "RNProxy";

public:
    struct Options
    {
        PipelineExecutorContext & exec_context;
        String req_id;
        Block header;
        SharedQueueSourceHolderPtr shared_queue;
    };

    explicit RNColumnarSourceOp(const Options & options)
        : SourceOp(options.exec_context, options.req_id)
        , shared_queue(options.shared_queue)
    {
        setHeader(options.header);
    }

    static SourceOpPtr create(const Options & options) { return std::make_unique<RNColumnarSourceOp>(options); }

    String getName() const override { return NAME; }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    OperatorStatus readImpl(Block & block) override;

private:
    SharedQueueSourceHolderPtr shared_queue;
};

} // namespace DB
#endif
