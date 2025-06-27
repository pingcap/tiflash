#include <Operators/CTEPartition.h>
#include <algorithm>
#include <mutex>
#include <utility>
#include <Common/Exception.h>

namespace DB
{
size_t CTEPartition::getIdxInMemoryNoLock(size_t cte_reader_id)
{
    if (this->total_block_in_disk_num >= this->fetch_block_idxs[cte_reader_id].idx)
        return this->fetch_block_idxs[cte_reader_id].idx;
    return this->fetch_block_idxs[cte_reader_id].idx - this->total_block_in_disk_num;
}

void CTEPartition::spillBlocks()
{
    std::lock_guard<std::mutex> lock(*(this->mu));

    for (const auto & block : this->tmp_blocks)
        this->blocks.push_back(block);
    this->tmp_blocks.clear();

    auto cte_reader_num = this->fetch_block_idxs.size();
    std::vector<size_t> split_idxs{0};
    split_idxs.reserve(cte_reader_num+1);
    for (auto iter : this->fetch_block_idxs)
        split_idxs.push_back(iter.second.idx);
    std::sort(split_idxs.begin(), split_idxs.end());

    auto begin_iter = this->blocks.begin();
    Blocks spilled_blocks;
    auto idx_num = split_idxs.size();
    for (size_t i = 0; i < idx_num; i++)
    {
        if (split_idxs[i] >= this->blocks.size())
            break;

        if (i == idx_num - 1)
            spilled_blocks.assign(begin_iter+split_idxs[i], this->blocks.end());    
        else
            spilled_blocks.assign(begin_iter+split_idxs[i], begin_iter+split_idxs[i+1]);

        
    }
}

void CTEPartition::getBlockFromDisk(size_t cte_reader_id, Block & block)
{
    std::lock_guard<std::mutex> lock(*(this->mu));
    RUNTIME_CHECK_MSG(this->isSpillTriggeredNoLock(), "Spill should be triggered");
    RUNTIME_CHECK_MSG(this->isBlockAvailableInDiskNoLock(cte_reader_id), "Requested block is not in disk");

    bool retry = false;
    while (true)
    {
        auto [iter, _] = this->cte_reader_restore_streams.insert(std::make_pair(cte_reader_id, nullptr));
        if (iter->second == nullptr)
        {
            auto spiller_iter = this->spillers.find(cte_reader_id);
            RUNTIME_CHECK_MSG(spiller_iter == this->spillers.end(), "cte reader {} can't find spiller", cte_reader_id);
            auto streams = spiller_iter->second->restoreBlocks(this->partition_id, 1);
            RUNTIME_CHECK(streams.size() == 1);
            iter->second = streams[0];
            iter->second->readPrefix();
        }

        block = iter->second->read();
        if (!block)
        {
            RUNTIME_CHECK(!retry);
            
            iter->second->readSuffix();
            iter->second = nullptr;
            retry = true;
            continue;
        }

        this->addIdxNoLock(cte_reader_id);
    }
}
} // namespace DB
