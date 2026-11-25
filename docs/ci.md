# CI and dependency automation

| File | Runs on | Does |
|---|---|---|
| `.github/workflows/build.yml` | Every push to a branch other than `main` | Tests and portable bundles |
| `.github/workflows/main.yml` | Every push to `main` | Tests and portable bundles, plus a nightly prerelease on every third push |
| `.github/workflows/reusable-tests.yml` | Called by the workflows above | Go and Rust tests on Linux, Windows and FreeBSD, plus protobuf checks |
| `.github/workflows/reusable-bundles.yml` | Called by the workflows above | Windows, Linux and FreeBSD `.tar.xz` bundles, kept as artifacts for 14 days |
| `.github/dependabot.yml` | Daily, and on new advisories | `auto/` branches for core dependencies |
| `.github/workflows/go-major-updates.yml` | Daily | `auto/go/` branches for new Go major versions |
| `.github/workflows/dependency-audit.yml` | Mondays | Read-only vulnerability audit |

Tests and bundles do not depend on each other, so a failing test still leaves artifacts to download. Toolchain setup lives in `.github/actions/setup-linux` and `.github/actions/setup-windows`, and FreeBSD setup lives in `scripts/ci/freebsd.sh`. Tests and bundles share all three, so they cannot drift apart.

## Nightlies

The push count is the `github.run_number` of `main.yml`. GitHub assigns it atomically, so concurrent pushes cannot race, and it needs no stored state. Only pushes to `main` advance it. When the number is divisible by 3 and the tests and all three bundles passed, the bundles are published as the prerelease `nightly-<number>` with a `SHA256SUMS` file.

- Re-running a failed run keeps its number, so a failed nightly can be retried.
- A third push that fails publishes nothing. The next chance is the sixth push.
- Pushes with `[skip ci]` are not counted, and renaming `main.yml` restarts the count.

## Dependency updates

Nothing merges automatically. Every path ends in a pull request that waits for a manual merge.

- **Dependabot** covers breaking Cargo releases, major Go releases that keep their module path, and security fixes. Dependabot's pushes trigger `build.yml`, which runs the tests and builds the bundles.
- **Go Major Updates** covers Go modules whose major version changes the module path, which Dependabot cannot do. It rewrites the imports with `scripts/ci/go_major_updates.py`, runs the tests, and opens a pull request, as a draft if the tests failed. To decline an update, close the pull request and keep the branch.

Core dependencies are the `allow` lists in `dependabot.yml` and `CORE_MODULES` in `go_major_updates.py`. Everything else appears only in the weekly audit.

## Repository settings

These cannot be set from the repository's files:

- Settings, Code security: turn on Dependabot alerts and Dependabot security updates.
- Settings, Actions, General: turn on "Allow GitHub Actions to create and approve pull requests", which Go Major Updates needs to open its pull requests.

Dependabot and scheduled workflows only run from the default branch, so they start once these files reach `main`.
