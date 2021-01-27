.. highlight:: console

Maintainer's Guides
-------------------

Toil maintainers accept pull requests from other Toil developers, and merge
them into the main line of Toil development. They are also responsible for
periodically producing releases.

.. _PRs:

Pull Requests
~~~~~~~~~~~~~
* All pull requests must be reviewed by a person other than the request's
  author. Review the PR by following the :ref:`reviewingPRs` checklist. 

* Modified pull requests must be re-reviewed before merging. **Note that Github
  does not enforce this!**
  
* Merge pull requests by following the :ref:`mergingPRs` checklist.

* When merging a pull request, make sure to update the `Draft Changelog`_ on
  the Github wiki, which we will use to produce the changelog for the next
  release. The PR template tells you to do this, so don't forget. New entries
  should go at the bottom.

  .. _Draft Changelog: https://github.com/DataBiosphere/toil/wiki/Draft-Changelog

* Pull requests will not be merged unless Travis and Gitlab CI tests pass.
  Gitlab tests are only run on code in the main Toil repository on some branch,
  so it is the responsibility of the approving reviewer to make sure that pull
  requests from outside repositories are copied to branches in the main
  repository. This can be accomplished with (from a Toil clone):

  .. code-block:: bash

      ./contrib/admin/test-pr theirusername their-branch issues/123-fix-description-here

  This must be repeated every time the PR submitter updates their PR, after
  checking to see that the update is not malicious.

  If there is no issue corresponding to the PR, after which the branch can be
  named, the reviewer of the PR should first create the issue.

  Developers who have push access to the main Toil repository are encouraged to
  make their pull requests from within the repository, to avoid this step.

* Prefer using "Squash and marge" when merging pull requests to master especially
  when the PR contains a "single unit" of work (i.e. if one were to rewrite the
  PR from scratch with all the fixes included, they would have one commit for
  the entire PR). This makes the commit history on master more readable
  and easier to debug in case of a breakage.

  When squashing a PR from multiple authors, please add
  `Co-authored-by`_ to give credit to all contributing authors.

  See `Issue #2816`_ for more details.

  .. _Co-authored-by: https://github.blog/2018-01-29-commit-together-with-co-authors/
  .. _Issue #2816: https://github.com/DataBiosphere/toil/issues/2816

Publishing a Release
~~~~~~~~~~~~~~~~~~~~

These are the steps to take to publish a Toil release:

* Determine the release version **X.Y.Z**. This should follow
  `semantic versioning`_; if user-workflow-breaking changes are made, **X**
  should be incremented, and **Y** and **Z** should be zero. If non-breaking
  changes are made but new functionality is added, **X** should remain the same
  as the last release, **Y** should be incremented, and **Z** should be zero.
  If only patches are released, **X** and **Y** should be the same as the last
  release and **Z** should be incremented.

  .. _semantic versioning: https://semver.org/

* If it does not exist already, create a release branch in the Toil repo
  named ``X.Y.x``, where **x** is a literal lower-case "x". For patch releases,
  find the existing branch and make sure it is up to date with the patch
  commits that are to be released. They may be `cherry-picked over`_ from
  master.

  .. _cherry-picked over: https://trunkbaseddevelopment.com/branch-for-release/

* On the release branch, edit ``version_template.py`` in the root of the
  repository. Find the line that looks like this (slightly different for patch
  releases):

  .. code-block:: python

      baseVersion = 'X.Y.0a1'

  Make it look like this instead:

  .. code-block:: python

      baseVersion = 'X.Y.Z'

  Commit your change to the branch.

* Tag the current state of the release branch as ``releases/X.Y.Z``.

* Make the Github release here_, referencing that tag. For a non-patch
  release, fill in the description with the changelog from `the wiki page`_,
  which you should clear. For a patch release, just describe the patch.

  .. _here: https://github.com/DataBiosphere/toil/releases/new
  .. _the wiki page: https://github.com/DataBiosphere/toil/wiki/Draft-Changelog

* For a non-patch release, set up the main branch so that development
  builds will declare themselves to be alpha versions of what the next release
  will probably be. Edit  ``version_template.py`` in the root of the repository
  on the main branch to set ``baseVersion`` like this:

  .. code-block:: python

      baseVersion = 'X.Y+1.0a1'

  Make sure to replace ``X`` and ``Y+1`` with actual numbers.

