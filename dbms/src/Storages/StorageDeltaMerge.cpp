#include <gperftools/malloc_extension.h>

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTinyLog.h>

#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>

namespace DB
{

StorageDeltaMerge::StorageDeltaMerge(const std::string & path_,
    const std::string & name_,
    const ColumnsDescription & columns_,
    const ASTPtr & primary_expr_ast_,
    Context & global_context_)
    : IStorage{columns_}, path(path_ + "/" + name_), name(name_), global_context(global_context_), log(&Logger::get("StorageDeltaMerge"))
{
    if (primary_expr_ast_->children.empty())
        throw Exception("No primary key");

    std::unordered_set<String> pks;
    for (size_t i = 0; i < primary_expr_ast_->children.size(); ++i)
    {
        auto col_name = primary_expr_ast_->children[0]->getColumnName();
        pks.emplace(col_name);
        pk_column_names.emplace_back(col_name);
    }

    size_t pks_combined_bytes = 0;
    auto all_columns = getColumns().getAllPhysical();
    size_t index = 0;
    for (auto & col : all_columns)
    {
        ColumnDefine column_define;
        column_define.name = col.name;
        column_define.type = col.type;
        column_define.id = index++;

        if (pks.count(col.name))
        {
            if (!col.type->isInteger())
                throw Exception("pk column " + col.name + " should be an integer");

            pks_combined_bytes += col.type->getSizeOfValueInMemory();
            if (pks_combined_bytes > sizeof(Handle))
                throw Exception("pk columns exceeds size limit :" + DB::toString(sizeof(Handle)));

            if (pks.size() == 1)
                handle_column_define = column_define;
        }

        table_column_defines.push_back(column_define);
        addColumn(header, column_define.id, col.name, col.type, col.type->createColumn());
    }

    if (pks.size() > 1)
    {
        handle_column_define.id = EXTRA_HANDLE_COLUMN_ID;
        handle_column_define.name = EXTRA_HANDLE_COLUMN_NAME;
        handle_column_define.type = EXTRA_HANDLE_COLUMN_TYPE;
    }

    store = std::make_shared<DeltaMergeStore>(
        global_context, path, name, table_column_defines, handle_column_define, DeltaMergeStore::Settings());
}

Block StorageDeltaMerge::buildInsertBlock(const Block & old_block)
{
    Block block = old_block;
    size_t rows = block.rows();
    if (!block.has(handle_column_define.name))
    {
        // put handle column.

        auto handle_column = handle_column_define.type->createColumn();
        handle_column->reserve(rows);
        auto & handle_data = typeid_cast<ColumnVector<Handle> &>(*handle_column).getData();

        size_t pk_column_count = pk_column_names.size();
        ColumnRawPtrs pk_columns;
        std::vector<size_t> pk_column_value_sizes;
        for (auto & n : pk_column_names)
        {
            auto & col = block.getByName(n);
            pk_columns.push_back(col.column.get());
            pk_column_value_sizes.push_back(col.type->getSizeOfValueInMemory());
        }

        for (size_t i = 0; i < rows; ++i)
        {
            Handle handle = 0;
            for (size_t c = 0; c < pk_column_count; ++c)
            {
                handle <<= (pk_column_value_sizes[c] << 3);
                handle |= pk_columns[c]->getUInt(i);
            }
            handle_data.push_back(handle);
        }

        addColumn(block, EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_TYPE, std::move(handle_column));
    }

    if (!block.has(VERSION_COLUMN_NAME))
    {
        auto column = VERSION_COLUMN_TYPE->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt64> &>(*column).getData();
        column_data.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            column_data[i] = next_version++;
        }

        addColumn(block, VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE, std::move(column));
    }

    if (!block.has(TAG_COLUMN_NAME))
    {
        auto column = TAG_COLUMN_TYPE->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt8> &>(*column).getData();
        column_data.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            column_data[i] = 0;
        }

        addColumn(block, TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE, std::move(column));
    }

    // Set the real column id.
    for (auto & col : block)
    {
        if (col.name != VERSION_COLUMN_NAME && col.name != TAG_COLUMN_NAME && col.name != EXTRA_HANDLE_COLUMN_NAME)
            col.column_id = header.getByName(col.name).column_id;
    }

    return block;
}

using BlockDecorator = std::function<Block(const Block &)>;
class DMBlockOutputStream : public IBlockOutputStream
{
public:
    DMBlockOutputStream(const DeltaMergeStorePtr & store_,
        const Block & header_,
        const BlockDecorator & decorator_,
        const Context & db_context_,
        const Settings & db_settings_)
        : store(store_), header(header_), decorator(decorator_), db_context(db_context_), db_settings(db_settings_)
    {}

    Block getHeader() const override { return header; }

    void write(const Block & block) override { store->write(db_context, db_settings, decorator(block)); }

private:
    DeltaMergeStorePtr store;
    Block header;
    BlockDecorator decorator;
    const Context & db_context;
    const Settings & db_settings;
};

BlockOutputStreamPtr StorageDeltaMerge::write(const ASTPtr & query, const Settings & settings)
{
    const auto * insert_query = typeid_cast<const ASTInsertQuery *>(&*query);
    if (!insert_query)
        throw Exception("Only support insert currently");
    BlockDecorator decorator = std::bind(&StorageDeltaMerge::buildInsertBlock, this, std::placeholders::_1);
    return std::make_shared<DMBlockOutputStream>(store, header, decorator, global_context, settings);
}


BlockInputStreams StorageDeltaMerge::read( //
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    ColumnDefines to_read;
    for (auto & n : column_names)
    {
        auto & column = header.getByName(n);
        ColumnDefine col_define;
        col_define.name = column.name;
        col_define.id = column.column_id;
        col_define.type = column.type;
        to_read.push_back(col_define);
    }

    return store->read(context, context.getSettingsRef(), to_read, max_block_size, num_streams, std::numeric_limits<UInt64>::max());
}


namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static ASTPtr extractKeyExpressionList(IAST & node)
{
    const ASTFunction * expr_func = typeid_cast<const ASTFunction *>(&node);

    if (expr_func && expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple.
        return expr_func->children.at(0);
    }
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node.ptr());
        return res;
    }
}

void registerStorageDeltaMerge(StorageFactory & factory)
{
    factory.registerStorage("DeltaMerge", [](const StorageFactory::Arguments & args) {
        if (args.engine_args.size() > 1)
            throw Exception("Engine DeltaMerge expects only one parameter. e.g. engine = DeltaMerge((a, b))");
        if (args.engine_args.size() < 1)
            throw Exception(
                "Engine DeltaMerge needs primary key. e.g. engine = DeltaMerge((a, b))", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTPtr primary_expr_list = extractKeyExpressionList(*args.engine_args[0]);
        return StorageDeltaMerge::create(args.data_path, args.table_name, args.columns, primary_expr_list, args.context);
    });
}

} // namespace DB