// Copyright 2026 PingCAP, Inc.
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

#include <Common/MyTime.h>
#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/File/ColumnStat.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileMetaV2.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
#include <Storages/DeltaMerge/Filter/And.h>
#include <Storages/DeltaMerge/Filter/DateQueryDomain.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/Index/RSIndex.h>
#include <Storages/DeltaMerge/Index/TrimMinMaxIndex.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <optional>

namespace DB::DM::tests
{
namespace
{
dtpb::TrimMinMaxIndexProps makeProps(UInt32 version, UInt64 lower, UInt64 upper, UInt64 pack_count)
{
    dtpb::TrimMinMaxIndexProps props;
    props.set_format_version(version);
    props.set_lower_bound(TrimMinMax::encodeBound(lower));
    props.set_upper_bound(TrimMinMax::encodeBound(upper));
    props.set_pack_count(pack_count);
    return props;
}
} // namespace

TEST(TrimMinMaxIndexPhaseA, PackMarkAccessors)
{
    PaddedPODArray<UInt8> pack_marks;
    PaddedPODArray<UInt8> has_value_marks;
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    auto minmaxes = type->createColumn();

    const UInt8 marks[] = {0x00, 0x01, 0x02, 0x04, 0x06, 0x07};
    for (UInt8 mark : marks)
    {
        pack_marks.push_back(mark);
        has_value_marks.push_back(1);
        minmaxes->insert(Field(static_cast<UInt64>(1)));
        minmaxes->insert(Field(static_cast<UInt64>(2)));
    }

    auto index = std::make_shared<MinMaxIndex>(std::move(pack_marks), std::move(has_value_marks), std::move(minmaxes));

    EXPECT_FALSE(index->hasNull(0));
    EXPECT_FALSE(index->hasTrimmedLow(0));
    EXPECT_FALSE(index->hasTrimmedHigh(0));

    EXPECT_TRUE(index->hasNull(1));
    EXPECT_FALSE(index->hasTrimmedLow(1));
    EXPECT_FALSE(index->hasTrimmedHigh(1));

    EXPECT_FALSE(index->hasNull(2));
    EXPECT_TRUE(index->hasTrimmedLow(2));
    EXPECT_FALSE(index->hasTrimmedHigh(2));

    EXPECT_FALSE(index->hasNull(3));
    EXPECT_FALSE(index->hasTrimmedLow(3));
    EXPECT_TRUE(index->hasTrimmedHigh(3));

    EXPECT_FALSE(index->hasNull(4));
    EXPECT_TRUE(index->hasTrimmedLow(4));
    EXPECT_TRUE(index->hasTrimmedHigh(4));

    EXPECT_TRUE(index->hasNull(5));
    EXPECT_TRUE(index->hasTrimmedLow(5));
    EXPECT_TRUE(index->hasTrimmedHigh(5));

    // Ordinary NULL checks must ignore trim bits (bit1/bit2 must not look like NULL).
    EXPECT_FALSE(hasNullMark(0x02));
    EXPECT_FALSE(hasNullMark(0x04));
    EXPECT_FALSE(hasNullMark(0x06));
    EXPECT_TRUE(hasNullMark(0x07));
}

TEST(TrimMinMaxIndexPhaseA, ValidateTrimPackMarks)
{
    PaddedPODArray<UInt8> ok;
    ok.push_back(0x00);
    ok.push_back(0x01);
    ok.push_back(0x02);
    ok.push_back(0x04);
    ok.push_back(0x07);
    EXPECT_TRUE(TrimMinMax::validateTrimPackMarks(ok, 5));

    PaddedPODArray<UInt8> bad_reserved;
    bad_reserved.push_back(0x08);
    EXPECT_FALSE(TrimMinMax::validateTrimPackMarks(bad_reserved, 1));

    PaddedPODArray<UInt8> bad_count;
    bad_count.push_back(0x01);
    EXPECT_FALSE(TrimMinMax::validateTrimPackMarks(bad_count, 2));
}

TEST(TrimMinMaxIndexPhaseA, ColumnStatProtoRoundTripAndUnknownField)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    ColumnStat stat{
        .col_id = 42,
        .type = type,
        .avg_size = 8,
        .serialized_bytes = 100,
        .data_bytes = 80,
        .mark_bytes = 8,
        .index_bytes = 20,
        .indexes = {},
        .trim_minmax_index = TrimMinMax::makeDefaultProps(*type, /*pack_count*/ 3),
    };

    auto proto = stat.toProto();
    ASSERT_TRUE(proto.has_trim_minmax_index());
    EXPECT_EQ(proto.trim_minmax_index().format_version(), TrimMinMax::FormatVersionV1);
    EXPECT_EQ(proto.trim_minmax_index().pack_count(), 3u);
    // Must not land in indexes=104.
    EXPECT_EQ(proto.indexes_size(), 0);

    ColumnStat restored;
    restored.mergeFromProto(proto);
    ASSERT_TRUE(restored.trim_minmax_index.has_value());
    EXPECT_EQ(restored.trim_minmax_index->pack_count(), 3u);
    EXPECT_EQ(restored.col_id, 42);

    // Simulate an old Reader that only knows fields up to indexes=104:
    // serialize with field 105, then parse into a message and clear unknown fields
    // by copying only known fields used by old code paths.
    String bytes;
    ASSERT_TRUE(proto.SerializeToString(&bytes));

    dtpb::ColumnStat old_reader_view;
    ASSERT_TRUE(old_reader_view.ParseFromString(bytes));
    // Current generated code knows field 105; verify wire bytes still round-trip when
    // field 105 is dropped (metadata rewrite by an old node).
    old_reader_view.clear_trim_minmax_index();
    ColumnStat after_old_rewrite;
    after_old_rewrite.mergeFromProto(old_reader_view);
    EXPECT_FALSE(after_old_rewrite.trim_minmax_index.has_value());
}

TEST(TrimMinMaxIndexPhaseA, TrySelectTrimMetaFallbackReasons)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    const auto lower = TrimMinMax::defaultLowerBoundPacked(*type);
    const auto upper = TrimMinMax::defaultUpperBoundPacked(*type);
    const String fname = colTrimIndexFileName("42");

    std::unordered_map<String, MergedSubFileInfo> merged;
    merged.emplace(fname, MergedSubFileInfo(fname, /*number*/ 0, /*offset*/ 10, /*size*/ 100));

    TrimMinMaxIndexMeta meta;
    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ false,
            makeProps(1, lower, upper, 2),
            *type,
            /*expected_pack_count*/ 2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::Disabled);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            std::nullopt,
            *type,
            2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::NoMeta);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            makeProps(/*version*/ 99, lower, upper, 2),
            *type,
            2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::UnsupportedVersion);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            makeProps(1, upper, lower, 2), // lower >= upper
            *type,
            2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::MetadataMismatch);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            makeProps(1, lower, upper, /*pack_count*/ 3),
            *type,
            /*expected*/ 2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::MetadataMismatch);

    auto props_ok = makeProps(1, lower, upper, 2);
    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            props_ok,
            *type,
            2,
            /*empty map*/ {},
            fname,
            &meta),
        TrimMinMaxFallbackReason::IndexMissing);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(/*read_enabled*/ true, props_ok, *type, 2, merged, fname, &meta),
        TrimMinMaxFallbackReason::None);
    EXPECT_EQ(meta.pack_count, 2u);
    EXPECT_EQ(meta.file_size, 100u);
    EXPECT_EQ(meta.lower_bound, lower);
    EXPECT_EQ(meta.upper_bound, upper);

    // Orphan subfile: meta missing but MergedSubFileInfo remains.
    EXPECT_TRUE(TrimMinMax::hasOrphanTrimSubFile(std::nullopt, merged, fname));
    EXPECT_FALSE(TrimMinMax::hasOrphanTrimSubFile(props_ok, merged, fname));
}

TEST(TrimMinMaxIndexPhaseA, FileNamingAndCacheKeyDistinct)
{
    EXPECT_EQ(colIndexFileName("42"), "42.idx");
    EXPECT_EQ(colTrimIndexFileName("42"), "42.trim.idx");
    EXPECT_NE(colIndexFileName("42"), colTrimIndexFileName("42"));
    EXPECT_TRUE(endsWith(colTrimIndexFileName("42"), details::TRIM_INDEX_FILE_SUFFIX));
    EXPECT_TRUE(endsWith(colTrimIndexFileName("42"), details::INDEX_FILE_SUFFIX));
}

TEST(TrimMinMaxIndexPhaseA, SettingsDefaultOff)
{
    Settings settings;
    EXPECT_FALSE(settings.dt_enable_trim_minmax);
}

TEST(TrimMinMaxIndexPhaseB, AddOrdinaryAndTrimPack)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    MinMaxIndex ordinary(*type);
    MinMaxIndex trim(*type);

    const UInt64 lower = TrimMinMax::defaultLowerBoundPacked(*type);
    const UInt64 upper = TrimMinMax::defaultUpperBoundPacked(*type);
    const UInt64 in_range = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 low_out = MyDateTime(1800, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 high_out = MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt();

    auto make_col = [&](const std::vector<UInt64> & vals) {
        auto col = type->createColumn();
        for (auto v : vals)
            col->insert(Field(v));
        return col;
    };

    // Pack 0: all in range
    {
        auto col = make_col({in_range, in_range + 1});
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_FALSE(ordinary.hasNull(0));
        EXPECT_TRUE(ordinary.getCell(0).has_value);
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_TRUE(trim.getCell(0).has_value);
    }

    // Pack 1: high outlier only
    {
        auto col = make_col({high_out});
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_TRUE(ordinary.getCell(1).has_value);
        EXPECT_FALSE(trim.getCell(1).has_value);
        EXPECT_FALSE(trim.hasTrimmedLow(1));
        EXPECT_TRUE(trim.hasTrimmedHigh(1));
    }

    // Pack 2: low + high + in-range
    {
        auto col = make_col({low_out, in_range, high_out});
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_TRUE(trim.getCell(2).has_value);
        EXPECT_TRUE(trim.hasTrimmedLow(2));
        EXPECT_TRUE(trim.hasTrimmedHigh(2));
        EXPECT_EQ(trim.getCell(2).min.safeGet<UInt64>(), in_range);
        EXPECT_EQ(trim.getCell(2).max.safeGet<UInt64>(), in_range);
    }

    // Pack 3: nullable NULL + high outlier
    {
        auto nullable_type = makeNullable(type);
        MinMaxIndex ordinary_n(*nullable_type);
        MinMaxIndex trim_n(*nullable_type);
        auto col = nullable_type->createColumn();
        col->insertDefault(); // NULL
        col->insert(Field(high_out));
        TrimMinMax::addOrdinaryAndTrimPack(ordinary_n, trim_n, *col, nullptr, lower, upper);
        EXPECT_TRUE(ordinary_n.hasNull(0));
        EXPECT_TRUE(trim_n.hasNull(0));
        EXPECT_TRUE(trim_n.hasTrimmedHigh(0));
        EXPECT_FALSE(trim_n.getCell(0).has_value);
        EXPECT_EQ(trim_n.packMark(0), PackMarkBits::Null | PackMarkBits::TrimmedHigh);
    }

    EXPECT_TRUE(trim.hasAnyTrimmedValue());
    EXPECT_FALSE(ordinary.hasAnyTrimmedValue());

    // Serialize / deserialize trim payload
    WriteBufferFromOwnString buf;
    trim.write(*type, buf);
    ReadBufferFromString rbuf(buf.str());
    auto restored = MinMaxIndex::read(*type, rbuf, buf.str().size());
    ASSERT_TRUE(TrimMinMax::validateTrimPackMarks(restored->packMarks(), 3));
    EXPECT_TRUE(restored->hasTrimmedHigh(1));
    EXPECT_TRUE(restored->hasTrimmedLow(2));
    EXPECT_TRUE(restored->hasTrimmedHigh(2));
}

// Design Validation Strategy: NULL-only, delete-mark, and no-valid-value packs.
TEST(TrimMinMaxIndexPhaseB, AddOrdinaryAndTrimPackNullDeleteAndEmpty)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    auto nullable_type = makeNullable(type);
    const UInt64 lower = TrimMinMax::defaultLowerBoundPacked(*type);
    const UInt64 upper = TrimMinMax::defaultUpperBoundPacked(*type);
    const UInt64 in_range = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 high_out = MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 low_out = MyDateTime(1800, 1, 1, 0, 0, 0, 0).toPackedUInt();

    // NULL only: has_null, no min/max, no low/high trimmed bits.
    {
        MinMaxIndex ordinary(*nullable_type);
        MinMaxIndex trim(*nullable_type);
        auto col = nullable_type->createColumn();
        col->insertDefault();
        col->insertDefault();
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);

        EXPECT_TRUE(ordinary.hasNull(0));
        EXPECT_FALSE(ordinary.hasValue(0));
        EXPECT_TRUE(trim.hasNull(0));
        EXPECT_FALSE(trim.hasValue(0));
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_EQ(trim.packMark(0), PackMarkBits::Null);
        EXPECT_FALSE(trim.hasAnyTrimmedValue());
    }

    // Delete mark + normal + outlier: deleted rows must not participate in min/max or flags.
    {
        MinMaxIndex ordinary(*type);
        MinMaxIndex trim(*type);
        auto col = type->createColumn();
        col->insert(Field(high_out)); // deleted high outlier
        col->insert(Field(in_range)); // live in-range
        col->insert(Field(low_out)); // deleted low outlier
        auto del_mark_col = createColumn<UInt8>({1, 0, 1}).column;
        const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());

        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, del_mark, lower, upper);

        EXPECT_TRUE(ordinary.hasValue(0));
        EXPECT_EQ(ordinary.getCell(0).min.safeGet<UInt64>(), in_range);
        EXPECT_EQ(ordinary.getCell(0).max.safeGet<UInt64>(), in_range);
        EXPECT_TRUE(trim.hasValue(0));
        EXPECT_EQ(trim.getCell(0).min.safeGet<UInt64>(), in_range);
        EXPECT_EQ(trim.getCell(0).max.safeGet<UInt64>(), in_range);
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_EQ(trim.packMark(0), 0);
        EXPECT_FALSE(trim.hasAnyTrimmedValue());
    }

    // No valid values: empty column.
    {
        MinMaxIndex ordinary(*type);
        MinMaxIndex trim(*type);
        auto col = type->createColumn();
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);

        EXPECT_FALSE(ordinary.hasValue(0));
        EXPECT_FALSE(ordinary.hasNull(0));
        EXPECT_FALSE(trim.hasValue(0));
        EXPECT_FALSE(trim.hasNull(0));
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_EQ(trim.packMark(0), 0);
        EXPECT_FALSE(trim.hasAnyTrimmedValue());
    }

    // No valid values: all rows deleted (including outliers that must be ignored).
    {
        MinMaxIndex ordinary(*type);
        MinMaxIndex trim(*type);
        auto col = type->createColumn();
        col->insert(Field(high_out));
        col->insert(Field(low_out));
        auto del_mark_col = createColumn<UInt8>({1, 1}).column;
        const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());

        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, del_mark, lower, upper);

        EXPECT_FALSE(ordinary.hasValue(0));
        EXPECT_FALSE(trim.hasValue(0));
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_EQ(trim.packMark(0), 0);
        EXPECT_FALSE(trim.hasAnyTrimmedValue());
    }

    // Deleted NULL must not set has_null; live outlier still sets trim flags.
    {
        MinMaxIndex ordinary(*nullable_type);
        MinMaxIndex trim(*nullable_type);
        auto col = nullable_type->createColumn();
        col->insertDefault(); // deleted NULL
        col->insert(Field(high_out)); // live high outlier
        auto del_mark_col = createColumn<UInt8>({1, 0}).column;
        const auto * del_mark = static_cast<const ColumnVector<UInt8> *>(del_mark_col.get());

        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, del_mark, lower, upper);

        EXPECT_FALSE(ordinary.hasNull(0));
        EXPECT_TRUE(ordinary.hasValue(0));
        EXPECT_FALSE(trim.hasNull(0));
        EXPECT_FALSE(trim.hasValue(0));
        EXPECT_TRUE(trim.hasTrimmedHigh(0));
        EXPECT_EQ(trim.packMark(0), PackMarkBits::TrimmedHigh);
        EXPECT_TRUE(trim.hasAnyTrimmedValue());
    }
}

TEST(TrimMinMaxIndexPhaseB, AppendPackRejectsInvalidMask)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    MinMaxIndex index(*type);
    EXPECT_THROW(
        index.appendPack(/*pack_mark*/ 0x08, /*has_value*/ false, PackMarkBits::TrimAllowedMask),
        DB::Exception);
    EXPECT_THROW(
        index
            .appendPack(/*pack_mark*/ PackMarkBits::TrimmedLow, /*has_value*/ false, PackMarkBits::OrdinaryAllowedMask),
        DB::Exception);
}

// Design Validation Strategy — Temporal-Type Tests:
// DATE / DATETIME(fsp) bounds, FSP near upper edge, zero/invalid packed values, Nullable.
TEST(TrimMinMaxIndexTemporalTypes, DateAndDateTimeBoundsFspAndCompatValues)
{
    auto expect_classification
        = [](const DataTypePtr & type, UInt64 value, bool expect_has_value, bool expect_low, bool expect_high) {
              MinMaxIndex ordinary(*type);
              MinMaxIndex trim(*type);
              const UInt64 lower = TrimMinMax::defaultLowerBoundPacked(*type);
              const UInt64 upper = TrimMinMax::defaultUpperBoundPacked(*type);
              auto col = type->createColumn();
              col->insert(Field(value));
              TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
              EXPECT_EQ(trim.hasValue(0), expect_has_value) << type->getName() << " value=" << value;
              EXPECT_EQ(trim.hasTrimmedLow(0), expect_low) << type->getName() << " value=" << value;
              EXPECT_EQ(trim.hasTrimmedHigh(0), expect_high) << type->getName() << " value=" << value;
              if (expect_has_value)
              {
                  EXPECT_EQ(trim.getCell(0).min.safeGet<UInt64>(), value);
                  EXPECT_EQ(trim.getCell(0).max.safeGet<UInt64>(), value);
              }
          };

    // Supported types: DATE, DATETIME(0/3/6), Nullable wrappers.
    EXPECT_TRUE(TrimMinMax::isSupportedTemporalType(*std::make_shared<DataTypeMyDate>()));
    EXPECT_TRUE(TrimMinMax::isSupportedTemporalType(*std::make_shared<DataTypeMyDateTime>(0)));
    EXPECT_TRUE(TrimMinMax::isSupportedTemporalType(*std::make_shared<DataTypeMyDateTime>(3)));
    EXPECT_TRUE(TrimMinMax::isSupportedTemporalType(*std::make_shared<DataTypeMyDateTime>(6)));
    EXPECT_TRUE(TrimMinMax::isSupportedTemporalType(*makeNullable(std::make_shared<DataTypeMyDate>())));
    EXPECT_TRUE(TrimMinMax::isSupportedTemporalType(*makeNullable(std::make_shared<DataTypeMyDateTime>(6))));

    // DATE and DATETIME default bounds use type-specific packing (FSPTT differs).
    auto date_type = std::make_shared<DataTypeMyDate>();
    auto dt0 = std::make_shared<DataTypeMyDateTime>(0);
    auto dt3 = std::make_shared<DataTypeMyDateTime>(3);
    auto dt6 = std::make_shared<DataTypeMyDateTime>(6);

    EXPECT_EQ(TrimMinMax::defaultLowerBoundPacked(*date_type), MyDate(1900, 1, 1).toPackedUInt());
    EXPECT_EQ(TrimMinMax::defaultUpperBoundPacked(*date_type), MyDate(2099, 12, 1).toPackedUInt());
    EXPECT_EQ(TrimMinMax::defaultLowerBoundPacked(*dt0), MyDateTime(1900, 1, 1, 0, 0, 0, 0).toPackedUInt());
    EXPECT_EQ(TrimMinMax::defaultUpperBoundPacked(*dt0), MyDateTime(2099, 12, 1, 0, 0, 0, 0).toPackedUInt());
    // FSP does not change the persisted default packed bounds.
    EXPECT_EQ(TrimMinMax::defaultLowerBoundPacked(*dt3), TrimMinMax::defaultLowerBoundPacked(*dt0));
    EXPECT_EQ(TrimMinMax::defaultUpperBoundPacked(*dt6), TrimMinMax::defaultUpperBoundPacked(*dt0));
    // TiFlash DATE / DATETIME share the same packed integer layout for midnight values.
    EXPECT_EQ(TrimMinMax::defaultLowerBoundPacked(*date_type), TrimMinMax::defaultLowerBoundPacked(*dt0));
    EXPECT_EQ(TrimMinMax::defaultUpperBoundPacked(*date_type), TrimMinMax::defaultUpperBoundPacked(*dt0));
    EXPECT_EQ(MyDate(2020, 1, 1).toPackedUInt(), MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt());

    // DATE: inclusive lower / exclusive upper / zero-date low outlier.
    expect_classification(date_type, MyDate(1900, 1, 1).toPackedUInt(), /*has*/ true, /*low*/ false, /*high*/ false);
    expect_classification(date_type, MyDate(2099, 11, 30).toPackedUInt(), true, false, false);
    expect_classification(date_type, MyDate(2099, 12, 1).toPackedUInt(), false, false, true);
    expect_classification(date_type, MyDate(2100, 1, 1).toPackedUInt(), false, false, true);
    expect_classification(date_type, MyDate(0, 0, 0).toPackedUInt(), false, true, false);
    // Invalid-date compatibility packed value still participates via packed compare (inside E).
    expect_classification(date_type, MyDate(2020, 2, 30).toPackedUInt(), true, false, false);

    // DATETIME(0/3/6): half-open E covers 2099-11-30 23:59:59.999999; 2099-12-01 is high.
    const UInt64 just_inside = MyDateTime(2099, 11, 30, 23, 59, 59, 999999).toPackedUInt();
    const UInt64 upper_edge = MyDateTime(2099, 12, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 lower_edge = MyDateTime(1900, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 zero_dt = MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt();
    for (const auto & type : {dt0, dt3, dt6})
    {
        expect_classification(type, lower_edge, true, false, false);
        expect_classification(type, just_inside, true, false, false);
        expect_classification(type, upper_edge, false, false, true);
        expect_classification(type, MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt(), false, false, true);
        expect_classification(type, zero_dt, false, true, false);
        // Fractional second inside a normal day stays in-range for every fsp.
        expect_classification(type, MyDateTime(2020, 1, 1, 12, 0, 0, 123456).toPackedUInt(), true, false, false);
    }

    // Nullable DATE: NULL + in-range value.
    {
        auto nullable_date = makeNullable(date_type);
        MinMaxIndex ordinary(*nullable_date);
        MinMaxIndex trim(*nullable_date);
        const UInt64 lower = TrimMinMax::defaultLowerBoundPacked(*nullable_date);
        const UInt64 upper = TrimMinMax::defaultUpperBoundPacked(*nullable_date);
        auto col = nullable_date->createColumn();
        col->insertDefault();
        col->insert(Field(MyDate(2020, 1, 1).toPackedUInt()));
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_TRUE(trim.hasNull(0));
        EXPECT_TRUE(trim.hasValue(0));
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_EQ(trim.getCell(0).min.safeGet<UInt64>(), MyDate(2020, 1, 1).toPackedUInt());
    }

    // NotNull DATE pack with only in-range values must not set null/low/high.
    {
        MinMaxIndex ordinary(*date_type);
        MinMaxIndex trim(*date_type);
        const UInt64 lower = TrimMinMax::defaultLowerBoundPacked(*date_type);
        const UInt64 upper = TrimMinMax::defaultUpperBoundPacked(*date_type);
        auto col = date_type->createColumn();
        col->insert(Field(MyDate(2020, 6, 1).toPackedUInt()));
        col->insert(Field(MyDate(2021, 6, 1).toPackedUInt()));
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_FALSE(ordinary.hasNull(0));
        EXPECT_FALSE(trim.hasNull(0));
        EXPECT_TRUE(trim.hasValue(0));
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_FALSE(trim.hasAnyTrimmedValue());
    }

    // Props encode/decode preserves DATE bounds.
    {
        auto props = TrimMinMax::makeDefaultProps(*date_type, /*pack_count*/ 4);
        EXPECT_EQ(props.format_version(), TrimMinMax::FormatVersionV1);
        EXPECT_EQ(TrimMinMax::decodeBound(props.lower_bound()), MyDate(1900, 1, 1).toPackedUInt());
        EXPECT_EQ(TrimMinMax::decodeBound(props.upper_bound()), MyDate(2099, 12, 1).toPackedUInt());
    }
}

// Eligibility: DATE calendar values and DATETIME FSP edge values against half-open E.
TEST(TrimMinMaxIndexTemporalTypes, DateQueryDomainUsesTypePackedBounds)
{
    const UInt64 date_lo = MyDate(1900, 1, 1).toPackedUInt();
    const UInt64 date_hi = MyDate(2099, 12, 1).toPackedUInt();
    const UInt64 dt_lo = MyDateTime(1900, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 dt_hi = MyDateTime(2099, 12, 1, 0, 0, 0, 0).toPackedUInt();

    DateQueryDomain d;
    d.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;

    d.values = {Field(MyDate(2020, 1, 1).toPackedUInt())};
    EXPECT_TRUE(d.isTrimEligible(date_lo, date_hi));
    d.values = {Field(MyDate(2099, 12, 1).toPackedUInt())};
    EXPECT_FALSE(d.isTrimEligible(date_lo, date_hi));
    d.values = {Field(MyDate(2100, 1, 1).toPackedUInt())};
    EXPECT_FALSE(d.isTrimEligible(date_lo, date_hi));
    d.values = {Field(MyDate(0, 0, 0).toPackedUInt())};
    EXPECT_FALSE(d.isTrimEligible(date_lo, date_hi));

    // Half-open E keeps 2099-11-30 23:59:59.999999 eligible; 2099-12-01 / 2100-01-01 are not.
    d.values = {Field(MyDateTime(2099, 11, 30, 23, 59, 59, 999999).toPackedUInt())};
    EXPECT_TRUE(d.isTrimEligible(dt_lo, dt_hi));
    d.values = {Field(MyDateTime(2099, 12, 1, 0, 0, 0, 0).toPackedUInt())};
    EXPECT_FALSE(d.isTrimEligible(dt_lo, dt_hi));
    d.values = {Field(MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt())};
    EXPECT_FALSE(d.isTrimEligible(dt_lo, dt_hi));

    // One-sided lower bound at the FSP edge remains eligible.
    d.predicate_class = TrimPredicateClass::LowerBounded;
    d.values.clear();
    d.lower = Field(MyDateTime(2099, 11, 30, 23, 59, 59, 999999).toPackedUInt());
    EXPECT_TRUE(d.isTrimEligible(dt_lo, dt_hi));
    d.lower = Field(MyDateTime(2099, 12, 1, 0, 0, 0, 0).toPackedUInt());
    EXPECT_FALSE(d.isTrimEligible(dt_lo, dt_hi));
}

class TrimMinMaxIndexWriteTest : public DB::base::TiFlashStorageTestBasic
{
protected:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        parent_path = getTemporaryPath();
        file_provider = db_context->getFileProvider();
    }

    static constexpr ColId settle_col_id = 100;

    static ColumnDefinesPtr makeColumns()
    {
        auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ false);
        cols->emplace_back(ColumnDefine{settle_col_id, "settle_time", std::make_shared<DataTypeMyDateTime>(0)});
        return cols;
    }

    static Block makeBlockWithOutlier(size_t rows)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, rows, /*reversed*/ false);
        auto type = std::make_shared<DataTypeMyDateTime>(0);
        auto col = type->createColumn();
        const UInt64 normal = MyDateTime(2020, 6, 1, 0, 0, 0, 0).toPackedUInt();
        const UInt64 sentinel = MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt();
        for (size_t i = 0; i < rows; ++i)
            col->insert(Field(i == 0 ? sentinel : normal));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "settle_time", settle_col_id));
        return block;
    }

    static Block makeBlockInRangeOnly(size_t rows)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, rows, /*reversed*/ false);
        auto type = std::make_shared<DataTypeMyDateTime>(0);
        auto col = type->createColumn();
        const UInt64 normal = MyDateTime(2020, 6, 1, 0, 0, 0, 0).toPackedUInt();
        for (size_t i = 0; i < rows; ++i)
            col->insert(Field(normal + i));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "settle_time", settle_col_id));
        return block;
    }

    DMFilePtr writeDMFile(const Block & block, bool enable_trim_write)
    {
        auto dm_file = DMFile::create(
            /*file_id*/ 1,
            parent_path,
            std::make_optional<DMChecksumConfig>(),
            128 * 1024,
            16 * 1024 * 1024,
            DMFileFormat::V3);
        auto cols = makeColumns();
        DMFileWriter::Options options;
        options.enable_trim_minmax = enable_trim_write;
        DMFileWriter writer(dm_file, *cols, file_provider, db_context->getWriteLimiter(), options);
        writer.write(block, DMFileWriter::BlockProperty{0, 0, 0, 0});
        writer.finalize();
        return dm_file;
    }

    String parent_path;
    FileProviderPtr file_provider;
};

TEST_F(TrimMinMaxIndexWriteTest, WriteTrimIndexWhenOutliersExist)
try
{
    auto dm_file = writeDMFile(makeBlockWithOutlier(/*rows*/ 8), /*enable_trim_write*/ true);
    ASSERT_TRUE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());
    EXPECT_EQ(dm_file->getColumnStat(settle_col_id).trim_minmax_index->pack_count(), dm_file->getPacks());
    EXPECT_EQ(dm_file->getColumnStat(settle_col_id).trim_minmax_index->format_version(), TrimMinMax::FormatVersionV1);

    const auto * meta = typeid_cast<const DMFileMetaV2 *>(dm_file->meta.get());
    ASSERT_NE(meta, nullptr);
    const auto fname = colTrimIndexFileName(DB::toString(settle_col_id));
    auto itr = meta->merged_sub_file_infos.find(fname);
    ASSERT_NE(itr, meta->merged_sub_file_infos.end());
    EXPECT_GT(itr->second.size, 0u);

    TrimMinMaxIndexMeta selected;
    auto reason = TrimMinMax::trySelectTrimMeta(
        /*read_enabled*/ true,
        dm_file->getColumnStat(settle_col_id).trim_minmax_index,
        *dm_file->getColumnStat(settle_col_id).type,
        dm_file->getPacks(),
        meta->merged_sub_file_infos,
        fname,
        &selected);
    EXPECT_EQ(reason, TrimMinMaxFallbackReason::None);
    EXPECT_EQ(selected.file_size, itr->second.size);

    // Restore and verify meta survives MetaV2 round-trip.
    auto restored = DMFile::restore(
        file_provider,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /*meta_version*/ 0);
    ASSERT_TRUE(restored->getColumnStat(settle_col_id).trim_minmax_index.has_value());
}
CATCH

TEST_F(TrimMinMaxIndexWriteTest, SkipPersistWhenNoOutliers)
try
{
    auto dm_file = writeDMFile(makeBlockInRangeOnly(/*rows*/ 8), /*enable_trim_write*/ true);
    EXPECT_FALSE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());

    const auto * meta = typeid_cast<const DMFileMetaV2 *>(dm_file->meta.get());
    ASSERT_NE(meta, nullptr);
    const auto fname = colTrimIndexFileName(DB::toString(settle_col_id));
    EXPECT_EQ(meta->merged_sub_file_infos.find(fname), meta->merged_sub_file_infos.end());
}
CATCH

TEST_F(TrimMinMaxIndexWriteTest, DisabledWriteSkipsTrim)
try
{
    auto dm_file = writeDMFile(makeBlockWithOutlier(/*rows*/ 8), /*enable_trim_write*/ false);
    EXPECT_FALSE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());

    const auto * meta = typeid_cast<const DMFileMetaV2 *>(dm_file->meta.get());
    ASSERT_NE(meta, nullptr);
    const auto fname = colTrimIndexFileName(DB::toString(settle_col_id));
    EXPECT_EQ(meta->merged_sub_file_infos.find(fname), meta->merged_sub_file_infos.end());
}
CATCH

TEST(TrimMinMaxIndexPhaseC, DateQueryDomainEligibility)
{
    // E = {date| date ∈ [1900-01-01 00:00:00, 2099-12-01 00:00:00)}
    const UInt64 e_lo = MyDateTime(1900, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 e_hi = MyDateTime(2099, 12, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 in_e = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 below = MyDateTime(1800, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 above = MyDateTime(2200, 1, 1, 0, 0, 0, 0).toPackedUInt();

    {
        // isTrimEligible == true when Q = {date | date ∈ {in_e}} (equality)
        DateQueryDomain d;
        d.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;
        d.values = {Field(in_e)};
        EXPECT_TRUE(d.isTrimEligible(e_lo, e_hi));
        d.values = {Field(above)};
        EXPECT_FALSE(d.isTrimEligible(e_lo, e_hi));
        // lower bound is inclusive, upper bound is exclusive.
        d.values = {Field(e_lo)};
        EXPECT_TRUE(d.isTrimEligible(e_lo, e_hi));
        d.values = {Field(e_hi)};
        EXPECT_FALSE(d.isTrimEligible(e_lo, e_hi));
    }
    {
        // isTrimEligible == true when Q = {date | date ∈ [in_e, in_e + 1]} (bounded range)
        DateQueryDomain d;
        d.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;
        d.lower = Field(in_e);
        d.upper = Field(in_e + 1);
        EXPECT_TRUE(d.isTrimEligible(e_lo, e_hi));
        // isTrimEligible == false when Q = {date | date ∈ [ine_e, above]} (bounded range)
        d.upper = Field(above);
        EXPECT_FALSE(d.isTrimEligible(e_lo, e_hi));
    }
    {
        // isTrimEligible == true when Q = {date | date ∈ [in_e, ∞)} (lower-bounded)
        DateQueryDomain d;
        d.predicate_class = TrimPredicateClass::LowerBounded;
        d.lower = Field(in_e);
        EXPECT_TRUE(d.isTrimEligible(e_lo, e_hi));
        // isTrimEligible == false when Q = {date | date ∈ [above, ∞)} (lower-bounded)
        d.lower = Field(above);
        EXPECT_FALSE(d.isTrimEligible(e_lo, e_hi));
    }
    {
        // isTrimEligible == true when Q = {date | date ∈ (-∞, in_e]} (upper-bounded)
        DateQueryDomain d;
        d.predicate_class = TrimPredicateClass::UpperBounded;
        d.upper = Field(in_e);
        EXPECT_TRUE(d.isTrimEligible(e_lo, e_hi));
        // isTrimEligible == false when Q = {date | date ∈ (-∞, below]} (upper-bounded)
        d.upper = Field(below);
        EXPECT_FALSE(d.isTrimEligible(e_lo, e_hi));
    }
}

TEST(TrimMinMaxIndexPhaseC, NormalizeMergesTemporalRange)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "t", .col_id = 7, .type = type};
    const UInt64 lo = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 hi = MyDateTime(2020, 1, 2, 0, 0, 0, 0).toPackedUInt();

    auto op = normalizeTemporalRangesForTrim(
        createAnd({createGreaterEqual(attr, Field(lo)), createLessEqual(attr, Field(hi))}));
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->name(), "date_range");
    auto reqs = op->getIndexRequests();
    ASSERT_EQ(reqs.size(), 1u);
    EXPECT_EQ(reqs[0].preferred_kind, RSIndexKind::PreferTrim);
    ASSERT_TRUE(reqs[0].query_domain.has_value());
    EXPECT_EQ(reqs[0].query_domain->predicate_class, TrimPredicateClass::EqualityOrInOrBounded);

    // OR must not rewrite children into PreferTrim DateRange.
    auto or_op = createOr({createGreaterEqual(attr, Field(lo)), createEqual(attr, Field(hi))});
    auto normalized_or = normalizeTemporalRangesForTrim(or_op);
    EXPECT_EQ(normalized_or->name(), "or");
}

// P1-2: unparseable temporal bounds must not be silently dropped into an empty DateRange.
TEST(TrimMinMaxIndexPhaseC, NormalizeKeepsUnparseableTemporalBounds)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "t", .col_id = 7, .type = type};
    const UInt64 hi = MyDateTime(2020, 1, 2, 0, 0, 0, 0).toPackedUInt();

    // NULL literal alone: keep original Greater, never emit empty DateRange.
    auto null_gt = normalizeTemporalRangesForTrim(createGreater(attr, Field()));
    ASSERT_NE(null_gt, nullptr);
    EXPECT_EQ(null_gt->name(), "greater");

    // Negative Int64 is also unparseable as UInt64 bound.
    auto neg_gt = normalizeTemporalRangesForTrim(createGreater(attr, Field(static_cast<Int64>(-1))));
    ASSERT_NE(neg_gt, nullptr);
    EXPECT_EQ(neg_gt->name(), "greater");

    // Mixed: one unparseable bound fails the whole column's DateRange merge.
    auto mixed = normalizeTemporalRangesForTrim(
        createAnd({createGreaterEqual(attr, Field()), createLessEqual(attr, Field(hi))}));
    ASSERT_NE(mixed, nullptr);
    EXPECT_EQ(mixed->name(), "and");
    auto and_op = std::dynamic_pointer_cast<And>(mixed);
    ASSERT_NE(and_op, nullptr);
    ASSERT_EQ(and_op->getChildren().size(), 2u);
    EXPECT_EQ(and_op->getChildren()[0]->name(), "greater_equal");
    EXPECT_EQ(and_op->getChildren()[1]->name(), "less_equal");
}

// Partial parse failure is per-column: failed column keeps originals; other columns still merge.
TEST(TrimMinMaxIndexPhaseC, NormalizePartialLeafParseFailureIsPerColumn)
{
    auto dt_type = std::make_shared<DataTypeMyDateTime>(0);
    auto int_type = std::make_shared<DataTypeInt64>();
    Attr t{.col_name = "t", .col_id = 7, .type = dt_type};
    Attr u{.col_name = "u", .col_id = 8, .type = dt_type};
    Attr c2{.col_name = "col_2", .col_id = 2, .type = int_type};

    const UInt64 lo = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 hi = MyDateTime(2020, 1, 2, 0, 0, 0, 0).toPackedUInt();

    // t >= NULL AND t <= hi  -> fail column t
    // u >= lo AND u <= hi    -> merge column u into DateRange
    // col_2 = 1              -> keep non-temporal leaf
    auto op = normalizeTemporalRangesForTrim(createAnd({
        createGreaterEqual(t, Field()),
        createLessEqual(t, Field(hi)),
        createGreaterEqual(u, Field(lo)),
        createLessEqual(u, Field(hi)),
        createEqual(c2, Field(static_cast<Int64>(1))),
    }));
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->name(), "and");
    auto and_op = std::dynamic_pointer_cast<And>(op);
    ASSERT_NE(and_op, nullptr);

    bool found_t_ge = false;
    bool found_t_le = false;
    bool found_u_date_range = false;
    bool found_c2_equal = false;
    bool found_t_date_range = false;

    for (const auto & child : and_op->getChildren())
    {
        if (child->name() == "greater_equal")
        {
            auto ids = child->getColumnIDs();
            ASSERT_EQ(ids.size(), 1u);
            EXPECT_EQ(ids[0], t.col_id);
            found_t_ge = true;
        }
        else if (child->name() == "less_equal")
        {
            auto ids = child->getColumnIDs();
            ASSERT_EQ(ids.size(), 1u);
            EXPECT_EQ(ids[0], t.col_id);
            found_t_le = true;
        }
        else if (child->name() == "date_range")
        {
            auto ids = child->getColumnIDs();
            ASSERT_EQ(ids.size(), 1u);
            if (ids[0] == t.col_id)
                found_t_date_range = true;
            else if (ids[0] == u.col_id)
            {
                found_u_date_range = true;
                auto reqs = child->getIndexRequests();
                ASSERT_EQ(reqs.size(), 1u);
                ASSERT_TRUE(reqs[0].query_domain.has_value());
                EXPECT_EQ(reqs[0].query_domain->predicate_class, TrimPredicateClass::EqualityOrInOrBounded);
                ASSERT_TRUE(reqs[0].query_domain->lower.has_value());
                ASSERT_TRUE(reqs[0].query_domain->upper.has_value());
                EXPECT_EQ(reqs[0].query_domain->lower->safeGet<UInt64>(), lo);
                EXPECT_EQ(reqs[0].query_domain->upper->safeGet<UInt64>(), hi);
            }
        }
        else if (child->name() == "equal")
        {
            auto ids = child->getColumnIDs();
            ASSERT_EQ(ids.size(), 1u);
            EXPECT_EQ(ids[0], c2.col_id);
            found_c2_equal = true;
        }
    }

    EXPECT_TRUE(found_t_ge);
    EXPECT_TRUE(found_t_le);
    EXPECT_FALSE(found_t_date_range);
    EXPECT_TRUE(found_u_date_range);
    EXPECT_TRUE(found_c2_equal);
    EXPECT_EQ(and_op->getChildren().size(), 4u); // t_ge, t_le, u_date_range, c2_equal
}

// P1-2: empty DateRange domain must return Some, never All.
TEST(TrimMinMaxIndexPhaseC, EmptyDateRangeDomainReturnsSome)
try
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "t", .col_id = 1, .type = type};
    const UInt64 v2021 = MyDateTime(2021, 1, 1, 0, 0, 0, 0).toPackedUInt();

    auto col = type->createColumn();
    col->insert(Field(v2021));
    MinMaxIndex ordinary(*type);
    ordinary.addPack(*col, nullptr);
    auto ordinary_ptr = std::make_shared<MinMaxIndex>(std::move(ordinary));

    RSCheckParam param;
    param.indexes.emplace(attr.col_id, RSIndex(type, ordinary_ptr));

    DateQueryDomain empty_domain;
    empty_domain.predicate_class = TrimPredicateClass::UpperBounded; // no lower/upper set
    auto empty_range = createDateRange(attr, empty_domain);
    auto res = empty_range->roughCheck(0, 1, param);
    ASSERT_EQ(res.size(), 1u);
    EXPECT_EQ(res[0], RSResult::Some);
}
CATCH

// Design Validation Strategy RSResult matrix: DateRange + Equal/In on the same packs.
TEST(TrimMinMaxIndexPhaseC, RoughCheckCorrectionDesignMatrix)
try
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "t", .col_id = 1, .type = type};
    const UInt64 e_lo = TrimMinMax::defaultLowerBoundPacked(*type);
    const UInt64 e_hi = TrimMinMax::defaultUpperBoundPacked(*type);
    const UInt64 v2021 = MyDateTime(2021, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v2020 = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v2022 = MyDateTime(2022, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v2100 = MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v1800 = MyDateTime(1800, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v2021_mid = MyDateTime(2021, 6, 1, 0, 0, 0, 0).toPackedUInt();

    auto make_col = [&](const std::vector<UInt64> & vals) {
        auto col = type->createColumn();
        for (auto v : vals)
            col->insert(Field(v));
        return col;
    };

    MinMaxIndex ordinary(*type);
    MinMaxIndex trim(*type);
    // pack0: {2021, 2100}
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v2021, v2100}), nullptr, e_lo, e_hi);
    // pack1: {2100}
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v2100}), nullptr, e_lo, e_hi);
    // pack2: {2021}
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v2021}), nullptr, e_lo, e_hi);
    // pack3: {1800}
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v1800}), nullptr, e_lo, e_hi);
    // pack4: {1800, 2100}
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v1800, v2100}), nullptr, e_lo, e_hi);

    constexpr size_t pack_count = 5;
    auto trim_ptr = std::make_shared<MinMaxIndex>(std::move(trim));
    RSCheckParam param;
    param.trim_indexes.emplace(
        attr.col_id,
        TrimRSIndex{
            .type = type,
            .minmax = trim_ptr,
            .meta = TrimMinMaxIndexMeta{
                .format_version = 1,
                .lower_bound = e_lo,
                .upper_bound = e_hi,
                .pack_count = pack_count}});

    auto expect_res = [](const RSResults & res, size_t pack, RSResult expected, const char * label) {
        ASSERT_LT(pack, res.size()) << label;
        EXPECT_EQ(res[pack], expected) << label << " got=" << fmt::format("{}", res[pack]);
        if (expected == RSResult::AllNull || expected == RSResult::SomeNull || expected == RSResult::Some)
            EXPECT_FALSE(res[pack].allMatch()) << label << " must keep row-level filtering";
    };

    // --- DateRange bounded: query=[2020, 2022] ---
    DateQueryDomain bounded;
    bounded.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;
    bounded.lower = Field(v2020);
    bounded.upper = Field(v2022);
    bounded.lower_inclusive = true;
    bounded.upper_inclusive = true;
    auto range_op = createDateRange(attr, bounded);
    auto bounded_res = range_op->roughCheck(0, pack_count, param);
    expect_res(bounded_res, 0, RSResult::Some, "DateRange [2020,2022] pack={2021,2100}");
    expect_res(bounded_res, 1, RSResult::None, "DateRange [2020,2022] pack={2100}");
    expect_res(bounded_res, 2, RSResult::All, "DateRange [2020,2022] pack={2021}");

    // --- DateRange lower-bounded: query>=2020 ---
    DateQueryDomain lower_b;
    lower_b.predicate_class = TrimPredicateClass::LowerBounded;
    lower_b.lower = Field(v2020);
    lower_b.lower_inclusive = true;
    auto ge_op = createDateRange(attr, lower_b);
    auto ge_res = ge_op->roughCheck(0, pack_count, param);
    expect_res(ge_res, 1, RSResult::Some, "DateRange >=2020 pack={2100}");
    expect_res(ge_res, 3, RSResult::None, "DateRange >=2020 pack={1800}");
    expect_res(ge_res, 4, RSResult::Some, "DateRange >=2020 pack={1800,2100}");

    // --- DateRange upper-bounded: query<=2020 ---
    DateQueryDomain upper_b;
    upper_b.predicate_class = TrimPredicateClass::UpperBounded;
    upper_b.upper = Field(v2020);
    upper_b.upper_inclusive = true;
    auto le_op = createDateRange(attr, upper_b);
    auto le_res = le_op->roughCheck(0, pack_count, param);
    expect_res(le_res, 3, RSResult::Some, "DateRange <=2020 pack={1800}");
    expect_res(le_res, 1, RSResult::None, "DateRange <=2020 pack={2100}");

    // --- Equal / In share EqualityOrInOrBounded correction on the same packs ---
    auto eq_op = createEqual(attr, Field(v2021));
    auto eq_res = eq_op->roughCheck(0, pack_count, param);
    expect_res(eq_res, 0, RSResult::Some, "Equal(2021) pack={2021,2100}");
    expect_res(eq_res, 1, RSResult::None, "Equal(2021) pack={2100}");
    expect_res(eq_res, 2, RSResult::All, "Equal(2021) pack={2021}");
    expect_res(eq_res, 3, RSResult::None, "Equal(2021) pack={1800}");
    expect_res(eq_res, 4, RSResult::None, "Equal(2021) pack={1800,2100}");

    auto in_op = createIn(attr, {Field(v2021), Field(v2021_mid)});
    auto in_res = in_op->roughCheck(0, pack_count, param);
    expect_res(in_res, 0, RSResult::Some, "in(2021,...) pack={2021,2100}");
    expect_res(in_res, 1, RSResult::None, "in(2021,...) pack={2100}");
    expect_res(in_res, 2, RSResult::All, "in(2021,...) pack={2021}");
    expect_res(in_res, 3, RSResult::None, "in(2021,...) pack={1800}");
    expect_res(in_res, 4, RSResult::None, "in(2021,...) pack={1800,2100}");

    // Single-value IN must match Equal on the shared correction matrix.
    auto in_single = createIn(attr, {Field(v2021)});
    EXPECT_EQ(in_single->roughCheck(0, pack_count, param), eq_res);

    // Bounded DateRange and Equal/In agree on shared EqualityOrInOrBounded packs.
    EXPECT_EQ(bounded_res[0], eq_res[0]);
    EXPECT_EQ(bounded_res[1], eq_res[1]);
    EXPECT_EQ(bounded_res[2], eq_res[2]);

    // pack={NULL,2021} must use a Nullable index: empty-value packs on Nullable MinMax
    // default to SomeNull (not None), so keep the NULL case on its own index.
    {
        auto nullable_type = makeNullable(type);
        Attr nullable_attr{.col_name = "t", .col_id = 1, .type = nullable_type};
        MinMaxIndex ordinary_n(*nullable_type);
        MinMaxIndex trim_n(*nullable_type);
        auto col = nullable_type->createColumn();
        col->insertDefault();
        col->insert(Field(v2021));
        TrimMinMax::addOrdinaryAndTrimPack(ordinary_n, trim_n, *col, nullptr, e_lo, e_hi);
        EXPECT_TRUE(trim_n.hasNull(0));
        EXPECT_TRUE(trim_n.hasValue(0));
        EXPECT_FALSE(trim_n.hasTrimmedLow(0));
        EXPECT_FALSE(trim_n.hasTrimmedHigh(0));

        auto trim_n_ptr = std::make_shared<MinMaxIndex>(std::move(trim_n));
        RSCheckParam nullable_param;
        nullable_param.trim_indexes.emplace(
            nullable_attr.col_id,
            TrimRSIndex{
                .type = nullable_type,
                .minmax = trim_n_ptr,
                .meta
                = TrimMinMaxIndexMeta{.format_version = 1, .lower_bound = e_lo, .upper_bound = e_hi, .pack_count = 1}});

        auto null_range = createDateRange(nullable_attr, bounded);
        auto null_range_res = null_range->roughCheck(0, 1, nullable_param);
        expect_res(null_range_res, 0, RSResult::AllNull, "DateRange [2020,2022] pack={NULL,2021}");
        EXPECT_NE(null_range_res[0], RSResult::All);

        auto null_eq = createEqual(nullable_attr, Field(v2021));
        auto null_eq_res = null_eq->roughCheck(0, 1, nullable_param);
        expect_res(null_eq_res, 0, RSResult::AllNull, "Equal(2021) pack={NULL,2021}");
        EXPECT_NE(null_eq_res[0], RSResult::All);

        auto null_in = createIn(nullable_attr, {Field(v2021), Field(v2021_mid)});
        auto null_in_res = null_in->roughCheck(0, 1, nullable_param);
        expect_res(null_in_res, 0, RSResult::AllNull, "in(2021,...) pack={NULL,2021}");
        EXPECT_EQ(null_range_res[0], null_eq_res[0]);
        EXPECT_EQ(null_eq_res[0], null_in_res[0]);
    }
}
CATCH

// Operator-level "Must fall back" shapes (no SQL): NotEqual / Not(DateRange) / Or(And range, IsNull).
TEST(TrimMinMaxIndexPhaseC, MustFallBackOperatorShapesHaveNoPreferTrim)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "t", .col_id = 1, .type = type};
    const UInt64 v2020 = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v2021 = MyDateTime(2021, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 e_lo = TrimMinMax::defaultLowerBoundPacked(*type);
    const UInt64 e_hi = TrimMinMax::defaultUpperBoundPacked(*type);

    auto has_prefer_trim = [](const RSOperatorPtr & op) {
        for (const auto & req : op->getIndexRequests())
        {
            if (req.preferred_kind == RSIndexKind::PreferTrim)
                return true;
        }
        return false;
    };

    EXPECT_FALSE(has_prefer_trim(createNotEqual(attr, Field(v2020))));

    DateQueryDomain bounded;
    bounded.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;
    bounded.lower = Field(v2020);
    bounded.upper = Field(v2021);
    bounded.lower_inclusive = true;
    bounded.upper_inclusive = true;
    auto date_range = createDateRange(attr, bounded);
    EXPECT_TRUE(has_prefer_trim(date_range));
    EXPECT_FALSE(has_prefer_trim(createNot(date_range)));

    // BETWEEN under OR is not rewritten to DateRange; GE/LE themselves are Normal.
    auto between_under_or = createOr(
        {createAnd({createGreaterEqual(attr, Field(v2020)), createLessEqual(attr, Field(v2021))}), createIsNull(attr)});
    EXPECT_FALSE(has_prefer_trim(between_under_or));
    EXPECT_EQ(normalizeTemporalRangesForTrim(between_under_or)->name(), "or");
    EXPECT_FALSE(has_prefer_trim(normalizeTemporalRangesForTrim(between_under_or)));

    // Out-of-E one-sided range remains PreferTrim at request time but is ineligible.
    DateQueryDomain out_e;
    out_e.predicate_class = TrimPredicateClass::LowerBounded;
    out_e.lower = Field(MyDateTime(2200, 1, 1, 0, 0, 0, 0).toPackedUInt());
    out_e.lower_inclusive = true;
    auto out_range = createDateRange(attr, out_e);
    ASSERT_TRUE(has_prefer_trim(out_range));
    ASSERT_TRUE(out_range->getIndexRequests()[0].query_domain.has_value());
    EXPECT_FALSE(out_range->getIndexRequests()[0].query_domain->isTrimEligible(e_lo, e_hi));
}

// NOT / NOT IN are outside trim support. Not must not PreferTrim, and must not let an empty
// trim pack turn In(..., NULL) into None then !None => All (skipping row filters).
TEST(TrimMinMaxIndexPhaseC, NotInWithNullDoesNotUseTrimToAdvertiseAll)
try
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "t", .col_id = 1, .type = type};
    const UInt64 e_lo = TrimMinMax::defaultLowerBoundPacked(*type);
    const UInt64 e_hi = TrimMinMax::defaultUpperBoundPacked(*type);
    const UInt64 v2020 = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v2200 = MyDateTime(2200, 1, 1, 0, 0, 0, 0).toPackedUInt();

    auto make_col = [&](const std::vector<UInt64> & vals) {
        auto col = type->createColumn();
        for (auto v : vals)
            col->insert(Field(v));
        return col;
    };

    MinMaxIndex ordinary(*type);
    MinMaxIndex trim(*type);
    // pack0: only out-of-E value — trim has_value=false, has_trimmed_high
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v2200}), nullptr, e_lo, e_hi);
    EXPECT_FALSE(trim.hasValue(0));
    EXPECT_TRUE(trim.hasTrimmedHigh(0));

    auto ordinary_ptr = std::make_shared<MinMaxIndex>(std::move(ordinary));
    auto trim_ptr = std::make_shared<MinMaxIndex>(std::move(trim));

    RSCheckParam param;
    param.indexes.emplace(attr.col_id, RSIndex(type, ordinary_ptr));
    param.trim_indexes.emplace(
        attr.col_id,
        TrimRSIndex{
            .type = type,
            .minmax = trim_ptr,
            .meta
            = TrimMinMaxIndexMeta{.format_version = 1, .lower_bound = e_lo, .upper_bound = e_hi, .pack_count = 1}});

    auto not_in_with_null = createNot(createIn(attr, {Field(v2020), Field()}));

    // Not must demote PreferTrim from In to Normal.
    auto reqs = not_in_with_null->getIndexRequests();
    ASSERT_EQ(reqs.size(), 1u);
    EXPECT_EQ(reqs[0].preferred_kind, RSIndexKind::Normal);
    EXPECT_FALSE(reqs[0].query_domain.has_value());

    // Ordinary semantics: In(2020, NULL) on {2200} => NoneNull, Not => AllNull (not All).
    // AllNull does not allMatch(), so row-level filter is retained.
    auto ordinary_only = param;
    ordinary_only.trim_indexes.clear();
    auto ordinary_res = not_in_with_null->roughCheck(0, 1, ordinary_only);
    ASSERT_EQ(ordinary_res.size(), 1u);
    EXPECT_EQ(ordinary_res[0], RSResult::AllNull);
    EXPECT_FALSE(ordinary_res[0].allMatch());

    // Even when trim is already loaded (sibling PreferTrim), Not must not advertise All.
    auto with_trim = not_in_with_null->roughCheck(0, 1, param);
    ASSERT_EQ(with_trim.size(), 1u);
    EXPECT_EQ(with_trim[0], RSResult::AllNull);
    EXPECT_NE(with_trim[0], RSResult::All);
    EXPECT_FALSE(with_trim[0].allMatch());

    // Without NULL, Not In(2020) on {2200} may correctly be All (all rows match).
    auto not_in = createNot(createIn(attr, {Field(v2020)}));
    auto not_in_res = not_in->roughCheck(0, 1, param);
    ASSERT_EQ(not_in_res.size(), 1u);
    EXPECT_EQ(not_in_res[0], RSResult::All);
}
CATCH

// P1-1: trim eligibility is per-predicate, not per-column.
// pack={2200}, predicate: col=2020 OR col=2200 must not be None.
TEST(TrimMinMaxIndexPhaseC, OrDoesNotShareTrimEligibilityAcrossPredicates)
try
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "t", .col_id = 1, .type = type};
    const UInt64 e_lo = TrimMinMax::defaultLowerBoundPacked(*type);
    const UInt64 e_hi = TrimMinMax::defaultUpperBoundPacked(*type);
    const UInt64 v2020 = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 v2200 = MyDateTime(2200, 1, 1, 0, 0, 0, 0).toPackedUInt();

    auto make_col = [&](const std::vector<UInt64> & vals) {
        auto col = type->createColumn();
        for (auto v : vals)
            col->insert(Field(v));
        return col;
    };

    MinMaxIndex ordinary(*type);
    MinMaxIndex trim(*type);
    // pack0: {2200} only — trim has no in-range value, has_trimmed_high
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v2200}), nullptr, e_lo, e_hi);
    // pack1: {2020} only
    TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *make_col({v2020}), nullptr, e_lo, e_hi);

    auto ordinary_ptr = std::make_shared<MinMaxIndex>(std::move(ordinary));
    auto trim_ptr = std::make_shared<MinMaxIndex>(std::move(trim));
    RSCheckParam param;
    param.indexes.emplace(attr.col_id, RSIndex(type, ordinary_ptr));
    param.trim_indexes.emplace(
        attr.col_id,
        TrimRSIndex{
            .type = type,
            .minmax = trim_ptr,
            .meta
            = TrimMinMaxIndexMeta{.format_version = 1, .lower_bound = e_lo, .upper_bound = e_hi, .pack_count = 2}});

    auto eq_in_e = createEqual(attr, Field(v2020));
    auto eq_out_e = createEqual(attr, Field(v2200));

    // Non-eligible Equal must ignore the column's loaded trim and use ordinary.
    auto out_res = eq_out_e->roughCheck(0, 2, param);
    EXPECT_EQ(out_res[0], RSResult::All); // {2200} matches
    EXPECT_EQ(out_res[1], RSResult::None); // {2020}

    // Eligible Equal may use trim.
    auto in_res = eq_in_e->roughCheck(0, 2, param);
    EXPECT_EQ(in_res[0], RSResult::None); // {2200} trimmed out
    EXPECT_EQ(in_res[1], RSResult::All); // {2020}

    const RSOperators or_ops = {
        createOr({eq_in_e, eq_out_e}),
        createOr({eq_out_e, eq_in_e}),
    };
    for (const auto & or_op : or_ops)
    {
        auto or_res = or_op->roughCheck(0, 2, param);
        EXPECT_NE(or_res[0], RSResult::None); // must keep pack with 2200
        EXPECT_NE(or_res[1], RSResult::None); // must keep pack with 2020
    }

    // IN containing an out-of-E value is not trim-eligible either.
    auto in_mixed = createIn(attr, {Field(v2020), Field(v2200)});
    auto in_mixed_res = in_mixed->roughCheck(0, 2, param);
    EXPECT_EQ(in_mixed_res[0], RSResult::All);
    EXPECT_EQ(in_mixed_res[1], RSResult::All);
}
CATCH

TEST_F(TrimMinMaxIndexWriteTest, PackFilterUsesTrimWhenReadEnabled)
try
{
    auto dm_file = writeDMFile(makeBlockWithOutlier(/*rows*/ 8), /*enable_trim_write*/ true);
    ASSERT_TRUE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());

    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Attr attr{.col_name = "settle_time", .col_id = settle_col_id, .type = type};
    const UInt64 lo = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 hi = MyDateTime(2021, 1, 1, 0, 0, 0, 0).toPackedUInt();
    DateQueryDomain domain;
    domain.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;
    domain.lower = Field(lo);
    domain.upper = Field(hi);
    auto filter = createDateRange(attr, domain);

    auto scan_context = std::make_shared<ScanContext>();
    auto pack_result = DMFilePackFilter::loadFrom(
        /*index_cache*/ nullptr,
        file_provider,
        /*read_limiter*/ nullptr,
        scan_context,
        dm_file,
        /*set_cache_if_miss*/ false,
        /*rowkey_ranges*/ {},
        filter,
        /*read_packs*/ {},
        /*tracing_id*/ "trim_phase_c",
        /*enable_trim_minmax*/ true,
        ReadTag::Query);

    // Sentinel-only values are trimmed out of min-max; bounded query must not be All.
    const auto & pack_res = pack_result->getPackRes();
    ASSERT_FALSE(pack_res.empty());
    for (auto r : pack_res)
        EXPECT_NE(r, RSResult::All);
}
CATCH

// P1-1 end-to-end: after an in-E Equal loads trim, an out-of-E Equal under OR must
// still load ordinary and keep packs that only contain the out-of-E value.
TEST_F(TrimMinMaxIndexWriteTest, PackFilterOrMixedEligibilityLoadsOrdinary)
try
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    Block block = DMTestEnv::prepareSimpleWriteBlock(0, 8, /*reversed*/ false);
    auto col = type->createColumn();
    const UInt64 out_e = MyDateTime(2200, 1, 1, 0, 0, 0, 0).toPackedUInt();
    for (size_t i = 0; i < 8; ++i)
        col->insert(Field(out_e));
    block.insert(ColumnWithTypeAndName(std::move(col), type, "settle_time", settle_col_id));

    auto dm_file = writeDMFile(block, /*enable_trim_write*/ true);
    ASSERT_TRUE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());

    Attr attr{.col_name = "settle_time", .col_id = settle_col_id, .type = type};
    const UInt64 in_e = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();

    const RSOperators filters = {
        createOr({createEqual(attr, Field(in_e)), createEqual(attr, Field(out_e))}),
        createOr({createEqual(attr, Field(out_e)), createEqual(attr, Field(in_e))}),
    };
    for (const auto & filter : filters)
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto pack_result = DMFilePackFilter::loadFrom(
            /*index_cache*/ nullptr,
            file_provider,
            /*read_limiter*/ nullptr,
            scan_context,
            dm_file,
            /*set_cache_if_miss*/ false,
            /*rowkey_ranges*/ {},
            filter,
            /*read_packs*/ {},
            /*tracing_id*/ "trim_phase_c_or",
            /*enable_trim_minmax*/ true,
            ReadTag::Query);

        const auto & pack_res = pack_result->getPackRes();
        ASSERT_FALSE(pack_res.empty());
        for (auto r : pack_res)
            EXPECT_NE(r, RSResult::None) << "out-of-E OR branch must keep packs with 2200";
    }
}
CATCH

// Attr.type may be empty when column id is missing from table defines.
// Equal/In must not dereference it in getIndexRequests (called unconditionally).
TEST(TrimMinMaxIndexSafety, EqualInNullAttrTypeDoesNotCrash)
{
    Attr attr{.col_name = "", .col_id = 42, .type = DataTypePtr{}};
    auto eq = createEqual(attr, Field(static_cast<UInt64>(1)));
    auto in = createIn(attr, {Field(static_cast<UInt64>(1)), Field(static_cast<UInt64>(2))});

    auto eq_reqs = eq->getIndexRequests();
    ASSERT_EQ(eq_reqs.size(), 1u);
    EXPECT_EQ(eq_reqs[0].preferred_kind, RSIndexKind::Normal);
    EXPECT_FALSE(eq_reqs[0].query_domain.has_value());

    auto in_reqs = in->getIndexRequests();
    ASSERT_EQ(in_reqs.size(), 1u);
    EXPECT_EQ(in_reqs[0].preferred_kind, RSIndexKind::Normal);
    EXPECT_FALSE(in_reqs[0].query_domain.has_value());

    RSCheckParam param;
    auto eq_res = eq->roughCheck(0, 1, param);
    ASSERT_EQ(eq_res.size(), 1u);
    EXPECT_EQ(eq_res[0], RSResult::Some);

    auto in_res = in->roughCheck(0, 1, param);
    ASSERT_EQ(in_res.size(), 1u);
    EXPECT_EQ(in_res[0], RSResult::Some);
}

} // namespace DB::DM::tests
