Changelog
=========

All notable changes to ``es-wait`` will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[0.15.1] - 2025-04-21
---------------------

Changed
~~~~~~~

- Dependency version bumps:
  - ``tiered_debug==1.3.0``
  - ``es_client>=8.18.2`` for tests only
  - ``furo>=2024.1.0`` for documentation only
- Updated _all_ files to use double quotes for strings, as per PEP 8 style guide, using Black formatter.
- Updated ``__init__.py`` to include additional metadata (``__author__``, ``__copyright__``, ``__license__``, ``__status__``, ``__description__``, ``__url__``, ``__email__``, ``__maintainer__``, ``__maintainer_email__``, ``__keywords__``, ``__classifiers__``) and dynamic copyright year handling.
- Updated ``docs/conf.py`` to use the Furo theme, add GitHub integration for "Edit Source" links, and enhance autodoc configuration with separated class signatures and typehint descriptions.
- Updated ``docs/requirements.txt`` to include ``furo>=2024.1.0`` for documentation generation.
- Split out the classes and modules from ``docs/api.rst`` into separate files for better organization and readability.

Impact
~~~~~~

- Improved project metadata consistency with comprehensive package information.
- Enhanced documentation with modern theme, better navigation, and GitHub integration.
- Ensured compatibility with updated dependencies.
- Improve documentation generation with Furo theme (I like the look better).

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
