.. highlight:: console

.. |X| raw:: html

    <input type="checkbox">

Pull Request Checklists
-----------------------

This document contains checklists for dealing with PRs. More general PR information is available at :ref:`PRs`.

.. _reviewingPRs:

Reviewing Pull Requests
~~~~~~~~~~~~~~~~~~~~~~~

This checklist is to be kept in sync with the checklist in the pull request template.

When reviewing a PR, do the following:

* |X| Make sure it is coming from ``issues/XXXX-fix-the-thing`` in the Toil repo, or from an external repo.
    * |X| If it is coming from an external repo, make sure to pull it in for CI with::
        
        contrib/admin/test-pr otheruser theirbranchname issues/XXXX-fix-the-thing
    * |X| If there is no associated issue, `create one <https://github.com/DataBiosphere/toil/issues/new>`_.
* |X| Read through the code changes. Make sure that it doesn't have:
    * |X| Addition of trailing whitespace.
    * |X| New variable or member names in ``camelCase`` that want to be in ``snake_case``.
    * |X| New functions without `type hints <https://docs.python.org/3/library/typing.html>`_.
    * |X| New functions or classes without informative docstrings.
    * |X| Changes to semantics not reflected in the relevant docstrings.
    * |X| New or changed command line options for Toil workflows that are not reflected in ``docs/running/cliOptions.rst``
    * |X| New features without tests.
* |X| Comment on the lines of code where problems exist with a review comment. You can shift-click the line numbers in the diff to select multiple lines.
* |X| Finish the review with an overall description of your opinion.

.. _mergingPRs:

Merging Pull Requests
~~~~~~~~~~~~~~~~~~~~~

This checklist is to be kept in sync with the checklist in the pull request template.

When merging a PR, do the following:

* |X| Make sure the PR passes tests.
* |X| Make sure the PR has been reviewed **since its last modification**. If not, review it.
* |X| Merge with the Github "Squash and merge" feature.
    * |X| If there are multiple authors' commits, add `Co-authored-by`_ to give credit to all contributing authors.
        .. _Co-authored-by: https://github.blog/2018-01-29-commit-together-with-co-authors/
* |X| Copy its recommended changelog entry to the `Draft Changelog <https://github.com/DataBiosphere/toil/wiki/Draft-Changelog>`_.
* |X| Append the issue number in parentheses to the changelog entry.
