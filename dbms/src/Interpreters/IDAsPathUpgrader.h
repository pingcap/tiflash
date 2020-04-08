#include <Core/Types.h>

#include <map>

namespace DB
{

class Context;

class IDAsPathUpgrader
{
    struct DatabaseDiskInfo
    {
        String path;
        String engine;
        Int64 id; // TODO: use DatabaseID
    };

public:
    IDAsPathUpgrader(Context & global_ctx_) : global_context(global_ctx_) {}

    bool needUpgrade();

    void doUpgrade();

private:
    void prepare();

private:
    Context & global_context;
    std::map<String, DatabaseDiskInfo> databases;
};

} // namespace DB
