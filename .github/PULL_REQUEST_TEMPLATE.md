## Changelog Entry
To be copied to the [draft changelog](https://github.com/DataBiosphere/toil/wiki/Draft-Changelog) by merger:

 * PR submitter writes their recommendation for a changelog entry here

## Reviewer Checklist

<!-- To be kept in sync with docs/contributing/checklist.rst -->

 * [ ] Make sure it is coming from `issues/XXXX-fix-the-thing` in the Toil repo, or from an external repo.
    * [ ] If it is coming from an external repo, make sure to pull it in for CI with:
        ```
        contrib/admin/test-pr otheruser theirbranchname issues/XXXX-fix-the-thing
        ```
    * [ ] If there is no associated issue, [create one](https://github.com/DataBiosphere/toil/issues/new).
* [ ] Read through the code changes. Make sure that it doesn't have:
    * [ ] Addition of trailing whitespace.
    * [ ] New variable or member names in `camelCase` that want to be in `snake_case`.
    * [ ] New functions without [type hints](https://docs.python.org/3/library/typing.html).
    * [ ] New functions or classes without informative docstrings.
    * [ ] Changes to semantics not reflected in the relevant docstrings.
    * [ ] New or changed command line options for Toil workflows that are not reflected in `docs/running/{cliOptions,cwl,wdl}.rst` 
    * [ ] New features without tests.
* [ ] Comment on the lines of code where problems exist with a review comment. You can shift-click the line numbers in the diff to select multiple lines.
* [ ] Finish the review with an overall description of your opinion.

## Merger Checklist

<!-- To be kept in sync with docs/contributing/checklist.rst -->

* [ ] Make sure the PR passes tests.
* [ ] Make sure the PR has been reviewed **since its last modification**. If not, review it.
* [ ] Merge with the Github "Squash and merge" feature.
    * [ ] If there are multiple authors' commits, add [Co-authored-by](https://github.blog/2018-01-29-commit-together-with-co-authors/) to give credit to all contributing authors.
* [ ] Copy its recommended changelog entry to the [Draft Changelog](https://github.com/DataBiosphere/toil/wiki/Draft-Changelog).
* [ ] Append the issue number in parentheses to the changelog entry.

