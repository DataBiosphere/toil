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
