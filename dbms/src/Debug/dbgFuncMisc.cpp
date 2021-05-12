#include <Debug/dbgFuncMisc.h>
#include <Interpreters/Context.h>

namespace DB
{
void dbgGetLogPath(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    auto path = context.getConfigRef().getString("logger.log");
    output(path);
}
} // namespace DB