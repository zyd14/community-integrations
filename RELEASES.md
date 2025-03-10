# Release Procedure

Building and releasing integrations is accomplished with GitHub actions where each library or integration has its own workflow.

For example, the `dagster-modal` integration has a workflow defined at `.github/workflows/release-dagster-modal.yml`.

Releases are triggered by creating a git tag of the pattern `<integration-name>-X.X.X`. For example:

    $ ./release.sh dagster-anthropic 0.0.2

### Variables

The GitHub action uses the `uv deploy` command, and requires the `UV_PUBLISH_TOKEN` and `UV_PUBLISH_URL` variables to be set.

For production deployment, `UV_PUBLISH_URL` must be set to:

- `https://upload.pypi.org/legacy/`

For test deployments, it can be set to:

- `https://test.pypi.org/simple/`

Maintainers are to ensure the variables are present in the _production_ environment of the [repository settings](https://github.com/dagster-io/community-integrations/settings).
