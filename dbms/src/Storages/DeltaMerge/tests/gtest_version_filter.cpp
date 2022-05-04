#include <Storages/DeltaMerge/tests/dm_basic_include.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>

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
    DebugBlockInputStream(const BlocksList & blocks) : begin(blocks.begin()), end(blocks.end()), it(blocks.begin()) {}
    String getName() const override { return "Debug"; }
    Block  getHeader() const override
    {
        auto cds = DMTestEnv::getDefaultColumns();
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
};

template <int MODE>
BlockInputStreamPtr genInputStream(const BlocksList & blocks, const ColumnDefines & columns, UInt64 max_version)
{
    ColumnDefine handle_define(TiDBPkColumnID, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_TYPE);
    return std::make_shared<DMVersionFilterBlockInputStream<MODE>>(std::make_shared<DebugBlockInputStream>(blocks), columns, max_version);
}

} // namespace

TEST(VersionFilter_test, MVCC)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 10, 0, str_col_name, "hello"));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 20, 0, str_col_name, "world"));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 30, 1, str_col_name, ""));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 0, str_col_name, "Flash"));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 40);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "Flash");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 30);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 0UL);
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 20);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "world");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 10);
        in->readPrefix();
        Block block = in->read();
        auto  col   = block.getByName(str_col_name);
        auto  val   = col.column->getDataAt(0);
        ASSERT_EQ(val, "hello");
        in->readSuffix();
    }
    {
        auto in = genInputStream<DM_VERSION_FILTER_MODE_MVCC>(blocks, columns, 9);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 0UL);
        in->readSuffix();
    }
}

TEST(VersionFilter_test, Compact)
{
    BlocksList blocks;
    // TODO fill this test
}

} // namespace tests
} // namespace DM
} // namespace DB
