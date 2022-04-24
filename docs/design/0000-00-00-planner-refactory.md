# planner refactory

- Author(s): [SeaRise](http://github.com/SeaRise)

## Introduction

Currently tiflash's interpreter is designed based on `DAGQueryBlock`.
`DAGQueryBlock` is designed to coprocess read, and it can handle simple queries well.
However, there is no way to handle the complex query of MPP, so the `DAGQueryBlock` tree is introduced, which is a compromise that makes the code more complicated and affects the subsequent iterative development.
We need to refactor the interpreter code in a plan visit way.

## Design

### Overview

we introduce a new abstraction layer PhysicalPlan to replace `DAGQueryBlock`.
It will be responsible for the processing of `ExpressionAction` and the generation of `BlockInputStream`.
We call this organization of code the `planner`.

why planner?
Most databases are designed based on planner.
- Friendly for database developers to understand code.
- We can improve tiflash with a general approach from the DB area, such as pipeline mode.

- flow chart
```
DAGRequest (executor tree or executor array).
     |
     |
     v
physical plan <---+
     |            |
     |            | optimize
     |            |
     +------------+   
     |
     |
     v
BlockInputStream
```

- code sample
```
/// tipb::executor → physical plan
PhysicalPlanBuilder physical_plan_builder;
traverseExecutorsReverse(
    dag_request,
    [&] (const tipb::executor & executor) { physical_plan_builder.append(executor); }
);
PhysicalPlan plan = physical_plan_builder.build();

/// optimize
std::vector<Rule> rules{..., FinalizeRule{}};
for (const Rule & rule : rules) 
    plan = rule.apply(plan); 

/// physical plan → BlockInputStream
DAGPipeline pipeline;
plan.transform(pipeline);
auto final_block_stream = executeUnion(pipeline);
```

### Details

#### physical plan

```
class physical plan
{
public:
    executor id
    output schema

    // 维护 ExpressionActions.finalize 逻辑
    void finalize(parent_require_columns);
    // 返回和实际 Block schema 一致的 empty column Block
    Block getSampleBlock();

    Children getChildren();


    // physical plan → BlockInputStream
    void transform(DAGPipeline &);

private:
    ExpressionActions
    …
}
```