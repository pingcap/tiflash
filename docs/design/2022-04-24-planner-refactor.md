# planner refactor

- Author(s): [SeaRise](http://github.com/SeaRise)

## Introduction

Currently tiflash's interpreter is designed based on `DAGQueryBlock`.
`DAGQueryBlock` is designed to coprocess read, and it can handle simple queries well.
However, there is no way to handle the complex query of MPP, so the `DAGQueryBlock` tree is introduced, which is a compromise that makes the code more complicated and affects the subsequent iterative development.
We need to refactor the interpreter code in a plan visit way.

## Design

### Overview

We introduce a new abstraction layer called `PhysicalPlan` to replace `DAGQueryBlock`.
It will be responsible for the processing of `ExpressionAction` and the generation of `BlockInputStream`.
We call this organization of code the `planner`.

In planner, a `DAGRequest` will be process as follows:
1. transform `DAGRequest` to `PhysicalPlan`
2. optimize `PhysicalPlan` (do the equivalent transformation on A to optimize the structure)
3. transform `PhysicalPlan` to `BlockInputStream`

#### why planner?
Most databases are designed based on planner.
- Friendly for database developers to understand code.
- We can improve tiflash with a general approach from the DB area, such as pipeline mode.

#### flow chart
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

#### code sample
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

### Detailed Design

#### PhysicalPlan

- basic structure
```
class PhysicalPlan
{
public:
    executor_id
    output_schema

    // for ExpressionActions.finalize
    void finalize(parent_require_columns);
    Block getSampleBlock();

    Children getChildren();

    // PhysicalPlan → BlockInputStream
    void transform(DAGPipeline &);

private:
    ExpressionActions
    …
}
```
- `finalize` and `getSampleBlock`
  - In order to continue the processing logic of ExpressionActionChain. 
    Because the physical plan handles the ExpressionAction instead of the ExpressionActionChain.
- output_schema
  - In order to maintain the mapping of tidb schema to tiflash block.
 
#### table scan with filter

In tiflash, if there is a filter on the table scan, the filter will be processed in the processing logic of the table scan.
In the planner, it is easy to handle this situation. We can convert the structure `filter --> table_scan` to PhysicalPlan `table_scan_with_filter`.

Process as shown below.
```
       xxxxxx                    xxxxxx
        |                         |
        |                         |
        v                         v
       Filter  ========> table scan with filter
        |                     
        |                 
        v                 
    table scan 
```

#### final projection

In the original implementation of tiflash, a projection will be appended at the end of DAGQueryBlock.
This projection is called `final projection`.

The role of the final projection
- for root query block:
  - Make the block and tidb schema consistent to ensure that the receiver can correctly parse the incoming block.
- for non-root query block:
  - To ensure that there are no duplicate column names in the left and right blocks obtained by join.
  - Because `ExpressionAction` assumes that there are no duplicate column names in the block.

In the planner, we need to insert the final projection operator in these places.
- Both sides of the join
- for root physical plan
  - If the root plan is ExchangeSender, insert the final projection below the root plan.
  - If not, insert above the root plan.

Process as shown below.
```
      xxxxxx                        final_projection       
        |                                |           
        |                                |           
        v                                v      
       join                            xxxxxx
        |                                |           
        |             ========>          |           
+-------+--------+                       v           
|                |                     join         
|                |                       |           
|                |                       |           
v                v               +-------+--------+  
child1        child2             |                |  
                                 |                |  
                                 |                |  
                                 v                v  
                        final_projection   final_projection
                                 |                |
                                 |                |
                                 |                |
                                 v                v
                               child1           child2
```

#### finalize

In order to allow the temporary columns generated by the computation process to be reused between different operators.
You can see the specific design in `ExpressionActionChain`.

Special treatment is needed for agg and join.
The semantics of aggregation dictates that the temporary columns generated by aggregation and the operators below aggregation cannot be used by the operators above aggregation.
Reusing temporary columns across joins is complicated.
So the temporary columns generated by the operators below aggregation and aggregation cannot be used by the operators above aggregation and aggregation.

Process as shown below.
```
f: finalize()
g: getSampleBlock()

         xxx
       |     ∧
      f|     |g
       v     |
         agg (cut off)
       |     ∧
      f|     |g
       v     |
         yyy
       |     ∧
      f|     |g
       v     |
         join (cut off)
      |  ∧  ∧  |
      |  |  |  |
  +---+ g|  |g +---+
 f|      |  |      |f
  |   +--+  +--+   |
  v   |        |   v
   aaa          bbb
```

#### optimize

In planner, there can be a simple optimizer that optimizes the physical plan tree.
Note that `FinalizeRule` must be the last rule.

```
class Rule
{
    PhysicalPlanPtr apply(const PhysicalPlanPtr &);
}

assert(rules.back().name == "finalize")
for (const auto & rule : rules)
    physical_plan = rule.apply(physical_plan);
```

### Test Design

We need a test framework to help testing the correctness of planner.

#### handwrite DAGRequest and PhysicalPlan

In order to test the logic of planner, we need to have easy-to-use methods to write the DAGRequest and physical plan by hand.
The sql like syntax is widely used to generate plan trees, such as spark rdd.
So handwritten DAGRequest and physical plan also use sql like syntax.

As shown below.
```
val left = 
    newMockTable("a", {"col1", "col2"})
    .filter("col1 != 2 and col2 != 1")
    .orderBy("col1", (desc)false)
    .project({"col1 + 1 as col1", "col2 + 2 as col2"});

val right = 
    newMockTable("b", {"col1", "col2"})
    .topN("col1", 5, (desc)true);

val mock_executor = 
    left.join(right, "a.col1 = b.col2")
    .project({"a.col1", "b.col2"})
    .build();
```

#### Check the transformation logic that DAGRequest to PhysicalPlan and PhysicalPlan to BlockInputStream

As shown below.
```
// DAGRequest -> PhysicalPlan
DAGRequest mock_input = xxxxxxxx;
PhysicalPlan result = planner.transform(mock_input);
PhysicalPlan expect = yyyyyyy;
checkEquals(result, expect);

// DAGRequest -> PhysicalPlan
PhysicalPlan mock_input = xxxxxxxx;
BlockInputStream result = planner.transform(mock_input);
BlockInputStream expect = yyyyyyy;
checkEquals(result, expect);
```

#### Check the execution result of the BlockInputStream generated by the planner

We can build a physical plan tree like this:
```
    mock-sink
        |
        |
        |
      join
        |
        |
   +----+-----+
   |          |
   |          |
   |          |
mock-source  mock-source
```
- mock-source: return the preset block
- mock-sink: write the result block to the given vector

We can then check that the output of the BlockInputStream generated by planner is as expected.
```
val mock_physical_plan = xxxx;
val mock_inputstream = planner.transform(mock_physical_plan);

val mock_input_block = xxxxxxx;
mock_inputstream.setInputBlock(mock_input_block);

val result_block = mock_inputstream.read();
val expect_block = xxxxxxxxxxx;

checkEquals(result_block, expect_block);
```