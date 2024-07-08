// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,n
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/dtpb/dmfile.pb.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/tests/gtest_dm_vector_index_utils.h>
#include <TiDB/Schema/VectorIndex.h>
#include <benchmark/benchmark.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <filesystem>
#include <highfive/highfive.hpp>
#include <optional>
#include <random>

namespace DB::DM::bench
{

/**
 * @brief Compatible with datasets on ANN-Benchmark:
 * https://github.com/erikbern/ann-benchmarks
 */
class Dataset
{
public:
    explicit Dataset(std::string_view file_name)
    {
        auto dataset_directory = std::filesystem::path(__FILE__).parent_path().string() + "/bench_dataset";
        auto dataset_path = fmt::format("{}/{}", dataset_directory, file_name);

        if (!std::filesystem::exists(dataset_path))
        {
            throw Exception(fmt::format(
                "Benchmark cannot run because dataset file {} not found. See {}/README.md for setup instructions.",
                dataset_path,
                dataset_directory));
        }

        auto file = HighFive::File(dataset_path, HighFive::File::ReadOnly);

        auto dataset_train = file.getDataSet("train");
        dataset_train.read(data_train);

        auto dataset_test = file.getDataSet("test");
        dataset_test.read(data_test);
    }

    virtual ~Dataset() = default;

    virtual UInt32 dimension() const = 0;

    virtual tipb::VectorDistanceMetric distanceMetric() const = 0;

public:
    MutableColumnPtr buildDataTrainColumn(std::optional<size_t> max_rows = std::nullopt) const
    {
        auto vec_column = ColumnArray::create(ColumnFloat32::create());
        size_t rows = data_train.size();
        if (max_rows.has_value())
            rows = std::min(rows, *max_rows);
        for (size_t i = 0; i < rows; ++i)
        {
            const auto & row = data_train[i];
            vec_column->insertData(reinterpret_cast<const char *>(row.data()), row.size() * sizeof(Float32));
        }
        return vec_column;
    }

    size_t dataTestSize() const { return data_test.size(); }

    const std::vector<Float32> & dataTestAt(size_t index) const { return data_test.at(index); }

    TiDB::VectorIndexDefinitionPtr createIndexDef(tipb::VectorIndexKind kind) const
    {
        return std::make_shared<const TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
            .kind = kind,
            .dimension = dimension(),
            .distance_metric = distanceMetric(),
        });
    }

protected:
    std::vector<std::vector<Float32>> data_train;
    std::vector<std::vector<Float32>> data_test;
};

class DatasetMnist : public Dataset
{
public:
    DatasetMnist()
        : Dataset("fashion-mnist-784-euclidean.hdf5")
    {
        RUNTIME_CHECK(data_train[0].size() == dimension());
        RUNTIME_CHECK(data_test[0].size() == dimension());
    }

    UInt32 dimension() const override { return 784; }

    tipb::VectorDistanceMetric distanceMetric() const override { return tipb::VectorDistanceMetric::L2; }

    static const DatasetMnist & get()
    {
        static DatasetMnist dataset;
        return dataset;
    }
};

class VectorIndexBenchUtils
{
public:
    template <typename Builder>
    static void saveVectorIndex(
        std::string_view index_path,
        const Dataset & dataset,
        std::optional<size_t> max_rows = std::nullopt)
    {
        Poco::File(index_path.data()).createDirectories();

        auto train_data = dataset.buildDataTrainColumn(max_rows);
        auto index_def = dataset.createIndexDef(Builder::kind());
        auto builder = std::make_unique<Builder>(index_def);
        builder->addBlock(*train_data, nullptr, []() { return true; });
        builder->save(index_path);
    }

    template <typename Viewer>
    static auto viewVectorIndex(std::string_view index_path, const Dataset & dataset)
    {
        auto index_view_props = dtpb::VectorIndexFileProps();
        index_view_props.set_index_kind(tipb::VectorIndexKind_Name(Viewer::kind()));
        index_view_props.set_dimensions(dataset.dimension());
        index_view_props.set_distance_metric(tipb::VectorDistanceMetric_Name(dataset.distanceMetric()));
        return Viewer::view(index_view_props, index_path);
    }

    static auto queryTopK(
        VectorIndexViewerPtr viewer,
        const std::vector<Float32> & ref,
        UInt32 top_k,
        std::optional<std::reference_wrapper<::benchmark::State>> state = std::nullopt)
    {
        if (state.has_value())
            state->get().PauseTiming();

        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        auto distance_metric = tipb::VectorDistanceMetric::INVALID_DISTANCE_METRIC;
        tipb::VectorDistanceMetric_Parse(viewer->file_props.distance_metric(), &distance_metric);
        ann_query_info->set_distance_metric(distance_metric);
        ann_query_info->set_top_k(top_k);
        ann_query_info->set_ref_vec_f32(DB::DM::tests::VectorIndexTestUtils::encodeVectorFloat32(ref));

        auto filter = BitmapFilterView::createWithFilter(viewer->size(), true);

        if (state.has_value())
            state->get().ResumeTiming();

        return viewer->search(ann_query_info, filter);
    }
};


} // namespace DB::DM::bench
