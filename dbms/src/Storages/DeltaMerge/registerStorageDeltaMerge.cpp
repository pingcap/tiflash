#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageFactory.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

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

static String getDeltaMergeVerboseHelp()
{
    String help = R"(

DeltaMerge requires:
- primary key
- an extra table info parameter in JSON format
- in most cases, it should be created implicitly through raft rather than explicitly

Examples of creating a DeltaMerge table:
- Create Table ... engine = DeltaMerge((CounterID, EventDate)) # JSON format table info is set to empty string
- Create Table ... engine = DeltaMerge((CounterID, EventDate), '{JSON format table info}')
)";
    return help;
}

void registerStorageDeltaMerge(StorageFactory & factory)
{
    factory.registerStorage("DeltaMerge", [](const StorageFactory::Arguments & args) {
        if (args.engine_args.size() > 2 || args.engine_args.empty())
            throw Exception(getDeltaMergeVerboseHelp(), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTPtr primary_expr_list = extractKeyExpressionList(*args.engine_args[0]);

        TiDB::TableInfo info;
        // Note: if `table_info_json` is not empty, `table_info` store a ref to `info`
        std::optional<std::reference_wrapper<const TiDB::TableInfo>> table_info = std::nullopt;
        if (args.engine_args.size() == 2)
        {
            auto ast = typeid_cast<const ASTLiteral *>(args.engine_args[1].get());
            if (ast && ast->value.getType() == Field::Types::String)
            {
                const auto table_info_json = safeGet<String>(ast->value);
                if (!table_info_json.empty())
                {
                    info.deserialize(table_info_json);
                    if (unlikely(info.columns.empty()))
                        throw Exception("Engine DeltaMerge table info is invalid. # of columns = 0", ErrorCodes::BAD_ARGUMENTS);
                    table_info = info;
                }
            }
            else
                throw Exception("Engine DeltaMerge table info must be a string" + getDeltaMergeVerboseHelp(), ErrorCodes::BAD_ARGUMENTS);
        }
        return StorageDeltaMerge::create(args.data_path, args.database_name, args.table_name, table_info, args.columns, primary_expr_list, args.context);
    });
}

} // namespace DB
