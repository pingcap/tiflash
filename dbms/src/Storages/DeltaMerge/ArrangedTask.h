#pragma once

class ArrangedTask
{
public:
    virtual ~ArrangedTask() = default;
    virtual void prepare() {}
    virtual void run() {}
};