#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/WindowDescription.h>



#include <deque>



namespace DB
{
class WindowBlockInputStream : public IProfilingBlockInputStream
{
public:
    WindowDescription window_description;
    WindowBlockInputStream(const BlockInputStreamPtr & input, const WindowDescription & window_description_) : window_description(window_description_)
    {
        children.push_back(input);
    }

    String getName() const override { return "Window"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;


};

}



