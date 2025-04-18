Changelog
=========

All notable changes to ``es-wait`` will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[0.15.0] - 2025-04-17
---------------------

Added
~~~~~

- Switched to Google-style docstrings to all classes and methods in all modules.
- Included ``Example`` sections for methods with >2 lines, with doctests where feasible.
- Added ``__repr__`` methods to waiter classes (``Relocate``, ``Snapshot``, ``Health``,
  ``Exists``, ``IndexLifecycle``, ``IlmPhase``, ``IlmStep``, ``Restore``, ``Task``) for
  better debugging.
- Updated, replaced or added Sphinx RST files (``index.rst``, ``installation.rst``,
  ``usage.rst``, ``api.rst``) for ReadTheDocs.

Changed
~~~~~~~

- Ensured all docstring lines are under 80 characters for linter compliance.
- Improved documentation clarity with consistent formatting and cross-references.
- Dependency version bumps:
   - ``tiered-debug==1.2.1``
   - ``es_client>=8.18.1`` for tests only.

Impact
~~~~~~

- Enhanced code maintainability with comprehensive, Sphinx-compatible docstrings.
- Improved debugging via ``__repr__`` methods showing key waiter attributes.
- Enabled professional ReadTheDocs documentation with API reference and examples.
