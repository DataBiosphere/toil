Contributing
============

Maintainer's Guidelines
-----------------------

* We strive to never break the build on master.

* Pull requests should be used for any and all changes (except truly trivial
  ones).

* The commit message of direct commits to master must end in ``(resolves #``
  followed by the issue number followed by ``)``.

Naming conventions
------------------

* The **branch name** for a pull request starts with ``issues/`` followed by the
  issue number (or numbers, separated by a dash), followed by a short
  snake-case description of the change. (There can be many open pull requests
  with their associated branches at any given point in time and this convention
  ensures that we can easily identify branches.)

* The **commit message** of the first commit in a pull request needs to end in
  ``(resolves #`` followed by the issue number, followed by ``)``. See `here`_
  for details about writing properly-formatted and informative commit messages.

* The title of the **pull request** needs to have the same ``(resolves #...)``
  suffix as the commit message. This lets `Waffle`_ stack the pull request
  and the associated issue. (Fortunately, Github automatically prepopulates the
  title of the PR with the message of the first commit in the PR, so this isn't
  any additional work.)

Say there is an issue numbered #123 titled `Foo does not work`. The branch name
would be ``issues/123-fix-foo`` and the title of the commit would be `Fix foo in
case of bar (resolves #123).`

* Pull requests that address **multiple issues** use the
  ``(resolves #602, resolves #214)`` suffix in the request's title. These pull
  requests can and should contain multiple commits, with each commit message
  referencing the specific issue(s) it addresses. We may or may not squash the
  commits in those PRs.

.. _here: http://chris.beams.io/posts/git-commit/
.. _Waffle: https://waffle.io/BD2KGenomics/toil

Pull requests
-------------

* All pull requests must be reviewed by a person other than the request's
  author.

* Only the reviewer of a pull request can merge it.

* Until the pull request is merged, it should be continually rebased by the
  author on top of master.

* Pull requests are built automatically by Jenkins and won't be merged unless
  all tests pass.

* Ideally, a pull request should contain a single commit that addresses a
  single, specific issue. Rebasing and squashing can be used to achieve that
  goal (see :ref:`multi-author`).

.. _multi-author:

Multi-author pull requests
--------------------------

* A pull request starts off as single-author and can be changed to multi-author
  upon request via comment (typically by the reviewer) in the PR. The author of
  a single-author PR has to explicitly grant the request.

* Multi-author pull requests can have more than one commit. They must `not` be
  rebased as doing so would create havoc for other contributors.

* To keep a multi-author pull request up to date with master, merge from master
  instead of rebasing on top of master.

* Before the PR is merged, it may transition back to single-author mode, again
  via comment request in the PR. Every contributor to the PR has to acknowledge
  the request after making sure they don't have any unpushed changes they care
  about. This is necessary because a single-author PR can be reabsed and
  rebasing would make it hard to integrate these pushed commits.
