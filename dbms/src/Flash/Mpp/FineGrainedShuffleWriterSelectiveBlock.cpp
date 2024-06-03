// // Copyright 2024 PingCAP, Inc.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
// 
// #include <Flash/Mpp/FineGrainedShuffleWriterSelectiveBlock.h>
// 
// namespace DB
// {
// 
// template <typename ExchangeWriterPtr>
// void FineGrainedShuffleWriterSelectiveBlock<ExchangeWriterPtr>::write(const Block & block)
// {
//     RUNTIME_CHECK(block.info.selective_ptr);
//     const auto selective_rows = block.info.selective->size();
//     RUNTIME_CHECK(selective_rows > 0);
// 
//     rows_to_send += selective_rows;
// 
//     // todo blocks.size() == fine_grained_shuffle_stream_count
//     if (rows_to_send > batch_send_row_limit)
//         batchWrite();
// }
// 
// template <typename ExchangeWriterPtr>
// void FineGrainedShuffleWriterSelectiveBlock<ExchangeWriterPtr>::flush()
// {
//     if (rows_to_send > 0)
//         batchWrite();
// }
// 
// template <typename ExchangeWriterPtr>
// void FineGrainedShuffleWriterSelectiveBlock<ExchangeWriterPtr>::batchWrite()
// {
//     assert(!blocks.empty());
//     assert(rows_to_send > 0);
// }
// 
// } // namespace DB
