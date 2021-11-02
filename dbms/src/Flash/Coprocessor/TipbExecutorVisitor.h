#pragma once

#include <tipb/select.pb.h>

namespace DB
{

class TipbExecutorVisitor
{
public:
    template <typename F>
    static bool bottomUpVisit(const tipb::Executor & e, F && func)
    {
        bool terminated = false;
        switch (e.tp())
        {
        case tipb::ExecType::TypeSelection:
            terminated = bottomUpVisit(e.selection().child(), std::forwared<F>(func));
            break;
        case tipb::ExecType::TypeAggregation:
        case tipb::ExecType::TypeStreamAgg:
            terminated = bottomUpVisit(e.aggregation().child(), std::forwared<F>(func));
            break;
        case tipb::ExecType::TypeTopN:
            terminated = bottomUpVisit(e.topn().child(), std::forwared<F>(func));
            break;
        case tipb::ExecType::TypeLimit:
            terminated = bottomUpVisit(e.limit().child(), std::forwared<F>(func));
            break;
        case tipb::ExecType::TypeJoin:
            terminated = bottomUpVisit(e.join().children(0), std::forwared<F>(func));
            if (!terminated)
                terminated = bottomUpVisit(e.join().children(1), std::forwared<F>(func));
            break;
        case tipb::ExecType::TypeExchangeSender:
            terminated = bottomUpVisit(e.exchange_sender().child(), std::forwared<F>(func));
            break;
        case tipb::ExecType::TypeProjection:
            terminated = bottomUpVisit(e.projection().child(), std::forwared<F>(func));
            break;
        default: break;
        }
        if (!terminated)
            terminated = func(e);
        return terminated;
    }

    template <typename F>
    static bool topDownVisit(const tipb::Executor & e, F && func)
    {
        bool terminated = func(e);
        if (!terminated)
        {
            switch (e.tp())
            {
            case tipb::ExecType::TypeSelection:
                terminated = topDownVisit(e.selection().child(), std::forwared<F>(func));
                break;
            case tipb::ExecType::TypeAggregation:
            case tipb::ExecType::TypeStreamAgg:
                terminated = topDownVisit(e.aggregation().child(), std::forwared<F>(func));
                break;
            case tipb::ExecType::TypeTopN:
                terminated = topDownVisit(e.topn().child(), std::forwared<F>(func));
                break;
            case tipb::ExecType::TypeLimit:
                terminated = topDownVisit(e.limit().child(), std::forwared<F>(func));
                break;
            case tipb::ExecType::TypeJoin:
                terminated = topDownVisit(e.join().children(0), std::forwared<F>(func));
                if (!terminated)
                    terminated = topDownVisit(e.join().children(1), std::forwared<F>(func));
                break;
            case tipb::ExecType::TypeExchangeSender:
                terminated = topDownVisit(e.exchange_sender().child(), std::forwared<F>(func));
                break;
            case tipb::ExecType::TypeProjection:
                terminated = topDownVisit(e.projection().child(), std::forwared<F>(func));
                break;
            default: break;
            }
        }
        return terminated;
    }
};
} // namespace DB
