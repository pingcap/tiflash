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
    AggregateDescriptions aggregate_descriptions;


};

}



