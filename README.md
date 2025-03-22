# Stage Model Builder (`smb`)

## What's the point?

When traversing lots of systems with dbt / duckdb for the London AICentre OMOP work it becomes necessary to make nice clean stage files from disparate systems. Said systems tend use a mixture of many databases (e.g. SQLServer, Oracle, so on) which can have wildly different (and quite annoying) naming conventions.

This package detects the names of columns in a parquet file and standardises them to the London AI Centre standard (`snake_case_style`).

## Installing

`uv pip install git+https://github.com/londonaicentre/smb.git`

## Using

`smb -h`
