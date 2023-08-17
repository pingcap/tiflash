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

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/copyData.h>


namespace DB
{
namespace
{
bool isAtomicSet(std::atomic<bool> * val)
{
    return ((val != nullptr) && val->load(std::memory_order_seq_cst));
}

} // namespace

template <typename TCancelCallback, typename TProgressCallback>
void copyDataImpl(
    IBlockInputStream & from,
    IBlockOutputStream & to,
    TCancelCallback && is_cancelled,
    TProgressCallback && progress)
{
    from.readPrefix();
    to.writePrefix();

    while (Block block = from.read())
    {
        if (is_cancelled())
            break;

        to.write(block);
        progress(block);
    }

    if (is_cancelled())
        return;

    /// For outputting additional information in some formats.
    if (auto * input = dynamic_cast<IProfilingBlockInputStream *>(&from))
    {
        if (input->getProfileInfo().hasAppliedLimit())
            to.setRowsBeforeLimit(input->getProfileInfo().getRowsBeforeLimit());

        to.setExtremes(input->getExtremes());
    }

    if (is_cancelled())
        return;

    from.readSuffix();
    to.writeSuffix();
}


inline void doNothing(const Block &) {}

void copyData(IBlockInputStream & from, IBlockOutputStream & to, std::atomic<bool> * is_cancelled)
{
    auto is_cancelled_pred = [is_cancelled]() {
        return isAtomicSet(is_cancelled);
    };

    copyDataImpl(from, to, is_cancelled_pred, doNothing);
}

void copyData(IBlockInputStream & from, IBlockOutputStream & to, const std::function<bool()> & is_cancelled)
{
    copyDataImpl(from, to, is_cancelled, doNothing);
}

void copyData(
    IBlockInputStream & from,
    IBlockOutputStream & to,
    const std::function<bool()> & is_cancelled,
    const std::function<void(const Block & block)> & progress)
{
    copyDataImpl(from, to, is_cancelled, progress);
}

} // namespace DB
