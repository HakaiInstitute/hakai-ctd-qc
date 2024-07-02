# CHANGELOG

## v0.3.0 (2024-07-02)

### Add

- Retrieve warnings from process_log
  - warning: slow do sensor for profiling application
  - warning: no soak detected
  - warning: lowered static cast threshold
- Include distance from station warning
- Integrate manual qc profile flags
- Add version test

## v0.2.0 (2023-11-03)

- Refactor code to rely in command line inputs or environment variables via the click package.
- Move package management to `poetry`` and [pyproject.toml](pyproject.toml)
- Reduce codebase
- Add pytest for nature trust sensor_submerged flags
