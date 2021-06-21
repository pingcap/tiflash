#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>

namespace DB
{
namespace DM
{
namespace tests
{

namespace
{

constexpr const char * str_col_name = "a";

class DebugBlockInputStream : public IProfilingBlockInputStream
{
public:
    DebugBlockInputStream(const BlocksList & blocks, bool is_common_handle_)
        : begin(blocks.begin()), end(blocks.end()), it(blocks.begin()), is_common_handle(is_common_handle_)
    {
    }
    String getName() const override { return "Debug"; }
    Block  getHeader() const override
    {
        auto cds = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        cds->push_back(ColumnDefine(100, str_col_name, DataTypeFactory::instance().get("String")));
        return toEmptyBlock(*cds);
    }

protected:
    Block readImpl() override
    {
        if (it == end)
            return Block();
        else
            return *(it++);
    }

private:
    BlocksList::const_iterator begin;
    BlocksList::const_iterator end;
    BlocksList::const_iterator it;
    bool                       is_common_handle;
};

template <int MODE>
BlockInputStreamPtr genInputStream(const BlocksList & blocks, const ColumnDefines & columns, UInt64 max_version, bool is_common_handle)
{
    ColumnDefine handle_define(
        TiDBPkColumnID, DMTestEnv::pk_name, is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE);
    return std::make_shared<DMVersionFilterBlockInputStream<MODE>>(
        std::make_shared<DebugBlockInputStream>(blocks, is_common_handle), columns, max_version, is_common_handle);
}

} // namespace

TEST(VersionFilter_test, MVCC)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 10, 0, str_col_name, "hello", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 20, 0, str_col_name, "world", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 30, 1, str_col_name, "", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 0, str_col_name, "Flash", false, 1));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 40, false);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "Flash");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 30, false);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 0UL);
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 20, false);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "world");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 10, false);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "hello");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 9, false);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 0UL);
        in->readSuffix();
    }
}

TEST(VersionFilter_test, MVCCCommonHandle)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 10, 0, str_col_name, "hello", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 20, 0, str_col_name, "world", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 30, 1, str_col_name, "", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 0, str_col_name, "Flash", true, 2));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 40, true);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "Flash");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 30, true);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 0UL);
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 20, true);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "world");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 10, true);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "hello");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 9, true);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 0UL);
        in->readSuffix();
    }
}

TEST(VersionFilter_test, Compact)
{
    // TODO: currently it just test data statistics, add test for data correctness
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 10, 0, str_col_name, "hello", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 20, 0, str_col_name, "world", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 30, 1, str_col_name, "", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 0, str_col_name, "Flash", false, 1));
        Int64 pk_value2 = 5;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value2, 10, 0, str_col_name, "hello", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value2, 20, 0, str_col_name, "world", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value2, 30, 1, str_col_name, "", false, 1));
        Int64 pk_value3 = 6;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value3, 10, 1, str_col_name, "hello", false, 1));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 40, false);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)1);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)0);
        ASSERT_EQ(gc_hint_version, (size_t)UINT64_MAX);
        in->readSuffix();
    }
    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 30, false);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)2);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 20, false);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)4);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 10, false);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)3);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)7);
        ASSERT_EQ(gc_hint_version, (size_t)10);
        in->readSuffix();
    }
}

TEST(VersionFilter_test, CompactCommonHandle)
{
    // TODO: currently it just test data statistics, add test for data correctness
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 10, 0, str_col_name, "hello", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 20, 0, str_col_name, "world", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 30, 1, str_col_name, "", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 0, str_col_name, "Flash", true, 2));
        Int64 pk_value2 = 5;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value2, 10, 0, str_col_name, "hello", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value2, 20, 0, str_col_name, "world", true, 2));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value2, 30, 1, str_col_name, "", true, 2));
        Int64 pk_value3 = 6;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value3, 10, 1, str_col_name, "hello", true, 2));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 40, true);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)1);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)0);
        ASSERT_EQ(gc_hint_version, (size_t)UINT64_MAX);
        in->readSuffix();
    }
    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 30, true);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)2);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 20, true);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)4);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto   in          = genInputStream<DM_VERSION_FILTER_MODE_COMPACT>(blocks, columns, 10, true);
        auto * mvcc_stream = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = UINT64_MAX;
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)3);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)7);
        ASSERT_EQ(gc_hint_version, (size_t)10);
        in->readSuffix();
    }
}

} // namespace tests
} // namespace DM
} // namespace DB
