#pragma once

#include <gtest/gtest.h>

#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
namespace tests
{

class TiFlashTestEnv
{
public:
    static String getTemporaryPath() { return "./tmp/"; }

    static Context & getContext(const DB::Settings & settings = DB::Settings())
    {
        static Context context = DB::Context::createGlobal();
        context.setPath(getTemporaryPath());
        context.setGlobalContext(context);
        context.initializeTiFlashMetrics();
        try
        {
            context.getTMTContext();
        }
        catch (Exception & e)
        {
            // set itself as global context
            context.setGlobalContext(context);
            context.setApplicationType(DB::Context::ApplicationType::SERVER);

            context.createTMTContext({}, "", "", {"default"}, getTemporaryPath() + "/kvstore", TiDB::StorageEngine::TMT, false);
            context.getTMTContext().restore();
        }
        context.getSettingsRef() = settings;
        return context;
    }
};

} // namespace tests
} // namespace DB
