#include <Common/ProfileEvents.h>
#include <TableFunctions/ITableFunction.h>


namespace ProfileEvents
{
extern const Event TableFunctionExecute;
}

namespace DB
{
StoragePtr ITableFunction::execute(const ASTPtr & ast_function, const Context & context) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    return executeImpl(ast_function, context);
}

} // namespace DB
