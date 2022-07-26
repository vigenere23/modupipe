[![Build](https://github.com/vigenere23/modupipe/actions/workflows/build.yml/badge.svg)](https://github.com/vigenere23/modupipe/actions/workflows/build.yml)

# ModuPipe : Modular pipelines made easy

ModuPipe is a ETL-like pipeline library that gives you an extensible and modular pipeline architecture ready to use.

## Benefits

- Entirely typed
- Abstract, so it fits any use case
- Class-based for easy configurations and injections

## Usage

Extract-Transform-Load (ETL) pipelines are a classic form of data-processing pipelines used in the industry. It consists of 3 main elements:

1. A **source**, which returns data in a stream-like structure (`Iterator` in Python)
2. A **mapper** (optional), which transforms (parse, converts, filters, etc.) the data obtained from the source(s)
3. A **sink**, which receives the maybe-transformed data (sinks can be multiple)

Therefore, those 3 processes are offered as interfaces, easily chainable and interchangeable at any time.

## Examples

Usage examples are present in the [examples](./examples) folder.

## Discussion

### Optimizing pushing to multiple sinks

If you have multiple sinks (using the `SinkList` class), but performance is a must, then you should use a multi-processing approach, and push to 1 queue per sink. Each queue will also become a direct source for each sink, all running in parallel. This is especially usefull when at least one of the sinks takes a long processing time.

As an example, let's take a `Sink1` which is very slow, and a `Sink2` which is normally fast. You'll be going from :

```
┌─── single pipeline ───┐
 Source ┬🠦 Sink1 (slow)
        └🠦 Sink2 (late)
```

to :

```
┌──── pipeline 1 ────┐             ┌──── pipeline 2 ─────┐
 Source ┬🠦 QueueSink1 ─🠦 Queue1 🠤─ QueueSource1 ─🠦 Sink1 (slow)
        └🠦 QueueSink1 ─🠦 Queue2 🠤─ QueueSource2 ─🠦 Sink2 (not late)
                                   └──── pipeline 3 ─────┘
```

This will of course not accelerate the `Sink1` processing time, but all the other sinks performances will be greatly improved by not waiting for each other.
