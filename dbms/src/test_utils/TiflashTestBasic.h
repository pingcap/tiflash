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
    static Context & getContext(const DB::Settings & settings = DB::Settings())
    {
        static Context context = DB::Context::createGlobal();
        context.setPath(".");
        context.setGlobalContext(context);
        try
        {
            context.getTMTContext();
        }
        catch (Exception & e)
        {
            // set itself as global context
            context.setGlobalContext(context);
            context.setApplicationType(DB::Context::ApplicationType::SERVER);

            context.createTMTContext({}, "", "", {"default"}, "./tmp/kvstore", "", TiDB::StorageEngine::TMT, false);
            context.getTMTContext().restore();
        }
        context.getSettingsRef() = settings;
        return context;
    }
};

} // namespace tests
} // namespace DB
