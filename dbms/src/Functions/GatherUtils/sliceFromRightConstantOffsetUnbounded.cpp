#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/Selectors.h>

namespace DB::GatherUtils
{
struct SliceFromRightConstantOffsetUnboundedSelectArraySource
    : public ArraySinkSourceSelector<SliceFromRightConstantOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset)
    {
        sliceFromRightConstantOffsetUnbounded(source, sink, offset);
    }
};

void sliceFromRightConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset)
{
    SliceFromRightConstantOffsetUnboundedSelectArraySource::select(src, sink, offset);
}
} // namespace DB::GatherUtils
