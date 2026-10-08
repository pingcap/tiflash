# Short-Circuit Evaluation of Logical Expressions

## Purpose

Replace the JSON_VALID-specific guard introduced by PR #11036 with deferred
expression evaluation. A guard must prevent execution of an unneeded expression
subtree, not change the error handling of a JSON cast inside that subtree.

The initial consumers are `and`, `or`, and `two_value_and`. The mechanism applies
to finalized expression actions, including filters, projections, and pushed-down
filters. It does not require or recognize JSON-specific expression patterns.

## Planning

`ExpressionActions::finalize()` marks lazy actions after output pruning and
column lifetime analysis. A reverse traversal records eager and deferred
consumers of each column. An expression may be deferred only if all its
consumers allow it and the function can execute on a filtered block. Result
columns remain eager.

The first argument of a logical function is required. Later arguments can be
deferred; if a logical function itself is deferred by an outer guard, its first
argument can also be deferred until that outer mask has been applied. Aliases
propagate consumer requirements. Projection, join, expansion, and nullable
conversion actions are conservative barriers.

Ordinary scalar functions are eligible if they are deterministic both globally
and within a query and suitable for constant folding. Block-dependent functions
such as `runningDifference` and `dumpColumnStructure`, random functions, and
side-effecting functions such as `sleep` remain eager. Custom `IFunctionBase`
implementations opt in explicitly. Functions with additional row or block
dependencies must override eligibility.

Actions that have not been finalized retain eager evaluation and materialized
intermediate columns. Appending an action to a finalized plan recomputes its
lazy marks. Type checking still happens when actions are built. Failed constant
value evaluation is retried at execution, where a parent may skip the
expression; logical, memory-limit, and query-cancellation errors are not
deferred.

## Execution

An eligible action captures its arguments in the existing `ColumnFunction`. Its
short-circuit flag distinguishes it from the existing lambda representation.
Logical functions maintain an active-row mask and evaluate arguments in action
argument order:

- `and` stops on a known false value.
- `or` stops on a known true value.
- SQL NULL does not saturate either three-valued operation.
- `two_value_and` treats NULL as false and stops on it.

`maskedExecute()` filters the captured inputs, reduces the expression on
selected rows, and expands its result to the original row positions. Expansion
uses the existing bulk column insertion methods rather than adding an interface
to every column implementation. Unselected result slots are placeholders used
only by the logical consumer; they are never fed through the skipped expression
subtree. An empty mask does not execute the expression; a full mask avoids
filtering and expansion. Logical functions without deferred arguments retain
their existing vectorized implementation.

Captured expressions remain immutable. Shared deferred expressions can be
evaluated under different masks without reusing a result from the wrong branch.
An independent eager consumer forces the shared producer to remain eager;
short-circuiting cannot suppress an error required by another output.

## ClickHouse Adaptation and Boundaries

The reference implementation is ClickHouse. TiFlash retains its linear actions,
existing column interfaces, and TiDB-specific logical and arithmetic semantics.

This change does not port ClickHouse's SQL setting or its cost-based
distinction between `enable` and `force_enable`. Eligible later-argument
subtrees are deferred by default. Filtering allocations and repeated execution
of shared subtrees are potential costs; a cost policy or mask-aware cache
requires separate benchmarks. This change also does not implement
`if`/`multiIf` branch masks or NULL-driven short-circuiting for ordinary
functions.

There is no guarantee that textual SQL predicate order survives TiDB planning.
Short-circuiting follows the expression order delivered to TiFlash and does not
reorder predicates to discover guards. Strict JSON parsing remains unchanged on
selected rows. `tidbDivide` continues returning NULL for division by zero as
required by TiDB semantics; that behavior is not the JSON guard being removed.
