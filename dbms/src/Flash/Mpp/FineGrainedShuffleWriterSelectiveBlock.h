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
// #pragma once
// 
// #include <Flash/Mpp/FineGrainedShuffleWriter.h>
// 
// namespace DB
// {
// template <typename ExchangeWriterPtr>
// class FineGrainedShuffleWriterSelectiveBlock : public FineGrainedShuffleWriter<ExchangeWriterPtr>
// {
// public:
//     FineGrainedShuffleWriterSelectiveBlock(
//             ExchangeWriterPtr writer_,
//             std::vector<Int64> partition_col_ids_,
//             TiDB::TiDBCollators collators_,
//             DAGContext & dag_context_,
//             uint64_t fine_grained_shuffle_stream_count_,
//             UInt64 fine_grained_shuffle_batch_size_,
//             MPPDataPacketVersion data_codec_version_,
//             tipb::CompressionMode compression_mode_)
//         : FineGrainedShuffleWriter<ExchangeWriterPtr>(
//                 writer_,
//                 std::move(partition_col_ids_),
//                 std::move(collators_),
//                 dag_context_,
//                 fine_grained_shuffle_stream_count_,
//                 fine_grained_shuffle_batch_size_,
//                 data_codec_version_,
//                 compression_mode_) {}
// 
//     void write(const Block & block) override;
//     void flush() override;
// 
// private:
//     void batchWrite();
// 
//     size_t rows_to_send = 0;
// };
// 
// } // namespace DB
