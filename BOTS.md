# Notes for AI Assistants

## Development Environment

The Python virtual environment is likely located at `./venv`, but this is just a guess - the user may have it elsewhere or may have already activated a virtualenv. If commands like `python` or `pytest` work directly, the environment is probably already active.

If you need to use the venv explicitly:

```bash
./venv/bin/python -m pytest src/toil/test/path/to/test.py -v
./venv/bin/python -c "import toil; print(toil.__version__)"
```

## Running Tests

Tests use pytest. Example commands:

```bash
# Run a specific test file
./venv/bin/python -m pytest src/toil/test/server/safeFileTest.py -v

# Run a specific test
./venv/bin/python -m pytest src/toil/test/server/safeFileTest.py::TestSafeFileInterleaving::test_reader_blocked_while_writer_holds_lock -v

# Run tests with a keyword filter
./venv/bin/python -m pytest src/toil/test -k "safe" -v
```

## Running Make Targets (mypy, tests, etc.)

The `Makefile` targets require the virtualenv to be activated. Some targets (like `test_debug`) enforce this with a `check_venv` guard. Use `source ./venv/bin/activate &&` before `make`:

```bash
source ./venv/bin/activate && make mypy
source ./venv/bin/activate && make test_debug tests='src/toil/test/path/to/test.py::TestClass::test_name'
```

## Running Individual WDL Spec Unit Tests

The WDL spec embeds example workflows as unit tests (under the `wdl-1.1` and `wdl-1.2` branches of `https://github.com/openwdl/wdl`). `TestWDLConformance.test_single_unit_test` in `src/toil/test/wdl/wdltoil_test.py` runs one such test at a time and is controlled by environment variables:

- `WDL_UNIT_TEST_ID`: which test to run (default: `glob_task`)
- `WDL_UNIT_TEST_VERSION`: WDL version (default: `1.1`)

```bash
WDL_UNIT_TEST_ID=serde_pair ./venv/bin/python -m pytest \
  src/toil/test/wdl/wdltoil_test.py::TestWDLConformance::test_single_unit_test \
  --timeout=300 -v -s
```

This test clones remote git repos and may be slow. Many WDL spec tasks run inside containers, so **Docker must be running** — if the test fails with a Docker connection error, ask the user to start Docker before retrying.
