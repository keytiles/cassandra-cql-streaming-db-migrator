In this codebase we follow our generic and Java-specific coding guidelines.

Please read the Markdown document https://raw.githubusercontent.com/keytiles/ai-agents/refs/heads/main/coding/java8-coding-rules-and-best-practices-v1.md and keep the rules and best practices it defines!

# Also consider the following

## Maven modules

In this repository we work with Maven with modules. Take a look into [pom.xml](../pom.xml) and see `<modules>`. Take the list of modules from there.

## jBehave integration tests (`pipeline` module)

Most jBehave tests live in the **`pipeline`** Maven module. Follow the generic jBehave rules in ai-agents (ASCII-only `.story` files, URL encoding in HTTP steps).

### Layout

| What | Path |
|------|------|
| Story files | `pipeline/test/stories/com/keytiles/jbehave/tests/` (package tree mirrors Java test packages) |
| Special / long-running stories | `pipeline/test/stories/com/keytiles/jbehave/tests_special/` |
| JUnit runners | `pipeline/src/test/java/com/keytiles/jbehave/tests/` |
| Shared base class | `pipeline/src/test/java/com/keytiles/jbehave/CommonKeytilesJBehaveTest.java` |
| Keytiles step definitions | `pipeline/src/test/java/com/keytiles/jbehave/steps/` |
| Composite step bundles | e.g. `pipeline/test/stories/com/keytiles/jbehave/tests/global-composite.steps`, `statquery-composite.steps`, `statquery-v2-composite.steps` (wired in `CommonKeytilesJBehaveTest`) |
| Entity / table row builders for Given data | `pipeline/src/test/java/com/keytiles/jbehave/entity/` |

### Naming and execution

- **`FooBarTest.java`** in `…/tests/httpStatQuery/` pairs with **`FooBarTest.story`** under `pipeline/test/stories/…/httpStatQuery/`.
- Run the **JUnit class** (e.g. `QueryTuningTest`), not the `.story` file directly.
- Runners extend `CommonKeytilesJBehaveTest`, use `@RunWith(SwfJBehaveJUnitReportingRunner.class)`, and wire steps in `stepsFactory()` (`KeytilesSteps`, `HttpSteps`, `ScyllaSteps`, module-specific steps such as `EventCountersSteps`, etc.).

### Story file conventions (this repo)

- Top-of-file `!--` block: what the story proves, fixed unix timestamps, container/auth notes when useful (see `QueryTuningTest.story` for a compact example).
- `Lifecycle: Before:` (and sometimes per-scenario setup) starts Scylla, `PipelineServer`, cache flush, and domain-specific Given steps before scenarios run.
- HTTP contract stories often assert status + JSON body fragments via `When the following HTTP request is sent` / `Then the following HTTP response is returned` tables (`HttpSteps`).

### Other modules

Some modules keep `resources/jbehave-override-properties/` for local overrides; see `pipeline/resources/jbehave-override-properties/README.md`. The bulk of stories and runners remains in `pipeline`.
