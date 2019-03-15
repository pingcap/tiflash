# Mutable supporting test framework

## *.test
```
This type of files is used to execute sql and check the output is matched.
(TODO)
```

## *.visual file:
```
This type of files is used to auto generate tests.
Each continuous rows is a testcase, describe a set of table parts, and keys in the parts.

  +: dedup target keys, have the same values
  -: irrelevant keys

Example:
  -++-
   +--

  ┌─────────── key = 1
  │┌────────── key = 2, dedup target key
  ││┌───────── key = 2, all dedup target keys are the same
  │││┌──────── no key, no row
  ││││
  -++          preset a table part with 3 rows: (key=1, val=random), (key=2, val=random), (key=2, val=random)
   +--         preset a table part with 3 rows: (key=2, val=random), (key=3, val=random), (key=4, val=random)
  ││││
  │││└──────── key = 4
  ││└───────── key = 3
  │└────────── key = 2, dedup target key
  └─────────── no key, no row

Two tests will be genarated from visual above, order 'part0, part1' and order 'part1, part0'.
The test should be a file like:

>> drop table if exists test
>> create table test (
	dt Date,
	k Int32,
	v Int32
	) engine = MutableMergeTree(dt, (k), 8192)

>> insert into test values (0, 1, 11), (0, 2, 22), (0, 2, 88)
>> insert into test values (0, 2, 66), (0, 3, 33), (0, 4, 44)

>> select * from test
┌─────────dt─┬─k─┬──v─┐
│ 0000-00-00 │ 1 │ 11 │
└────────────┴───┴────┘
┌─────────dt─┬─k─┬──v─┐
│ 0000-00-00 │ 2 │ 66 │
│ 0000-00-00 │ 3 │ 33 │
│ 0000-00-00 │ 4 │ 44 │
└────────────┴───┴────┘

>> selraw * from test
┌─────────dt─┬─k─┬──v─┬─_INTERNAL_VERSION─┬─_INTERNAL_DELMARK─┐
│ 0000-00-00 │ 1 │ 11 │           1000000 │                 0 │
│ 0000-00-00 │ 2 │ 22 │           1000001 │                 0 │
│ 0000-00-00 │ 2 │ 88 │           1000002 │                 0 │
└────────────┴───┴────┴───────────────────┴───────────────────┘
┌─────────dt─┬─k─┬──v─┬─_INTERNAL_VERSION─┬─_INTERNAL_DELMARK─┐
│ 0000-00-00 │ 2 │ 66 │           2000000 │                 0 │
│ 0000-00-00 │ 3 │ 33 │           2000001 │                 0 │
│ 0000-00-00 │ 4 │ 44 │           2000002 │                 0 │
└────────────┴───┴────┴───────────────────┴───────────────────┘

>> drop table if exists test
```
