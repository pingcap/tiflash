#pragma once
#include <Common/Exception.h>
#include <Functions/GatherUtils/ArraySinkVisitor.h>

namespace DB::GatherUtils
{
struct IArraySink
{
    virtual ~IArraySink() = default;

    virtual void accept(ArraySinkVisitor &)
    {
        throw Exception("Accept not implemented for " + demangle(typeid(*this).name()));
    }
};

template <typename Derived>
class ArraySinkImpl : public Visitable<Derived, IArraySink, ArraySinkVisitor>
{
};

} // namespace DB::GatherUtils
