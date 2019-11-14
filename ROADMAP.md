# Near Term (In Progress, Estimated Completion Date?)
*  Libcloud provisioning
   - [ ] Google
   - [ ] AWS
- [ ] Fix flaky tests
- [ ] Run massive workflows
- [ ] Better feedback (error messages, logging).

# Medium Term (~ 6-month goals, by ~June 2018?)
* Batch systems
   - [ ] Google Pipelines
   - [ ] AWS Batch
- [ ] Containerize leader (work with Consonance)
- [ ] Change the thread pool model to improve single machine usage.
* Improve the development process.
   - [ ] Add a linter
   - [ ] Add a code coverage tool.
   - [ ] Organize tests.
   - [ ] Better access to tests for external developers.
- [ ] TES support
- [ ] WES Support (if Consonance does not work well)

# Longer Term
- [ ] Better track versions of specifications (e.g. CWL, WDL) and dependencies.
- [ ] Add other provisioners: OpenStack
- [ ] Singularity support.
- [ ] Uniform configuration (i.e. not just environment variables).
- [ ] Add management and monitoring UIs.
- [ ] Python 3 support.
- [ ] Add URL wrapping for streaming instead of copying.

# Completed
- [x] Basic WDL support.
- [x] Travis CI for commits.
 - [x] Run Toil within Popper (https://cross.ucsc.edu/tag/popper/).
 - [x] Grafana for workflow monitoring
 - [x] Finish Google jobStore (GCP)
