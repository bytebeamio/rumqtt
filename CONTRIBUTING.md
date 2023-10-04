# Contributing to rumqtt

If you have loved using `rumqtt` and want to give back, we would love to have you open GitHub issues and PRs for features, bugs and documentation improvements.

We try to follow a fortnightly release cycle, with releases versioned in [semver](https://semver.org), maintaining a [changelog](./CHANGELOG.md) where changes are tracked and included within the PR where the changes are made. PRs shall only be squash merged and include a well written commit message with a format similar to the [Conventional Commits](https://www.conventionalcommits.org) to better describe the changes and their relevance. The message format convention we shall use is as follows:

```txt
<Tag>(<Component>): <Title>

[BREAKING:]

<Body>

Signed off: <Contributor>

[Issue:]

Attribute:
```

## Squash Commit Message Convention

-   `Tags` are used to describe the type of commit(e.g: `fix:`, `feat:`, `build:`, `chore:`, `ci:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`).
-   The optional use of `Component` describes the module or specific component to which changes included in the commit are associated with.
-   `Title` contains a brief description of the changes included in the commit, a single line summary.
-   An optional `BREAKING` label could also be included with a message describing the change that is breaking an API exposed by the project.
-   A compulsory `Body` must contain the descriptive explanation of the changes made within the commit and include any reasoning as to why they were included.
-   A further section `Signed off:` will denote that the associated contributors have signed-off the code contained in the commit.
-   An optional `Issue` section could describe any GitHub Issue associated with the commit.
-   `Attribute` section is used to tag contributors to the PR.

`rumqtt` is licensed under the permissive [Apache License Version 2.0](./LICENSE) and we accept contributions under the implied notion that they are made in complete renunciation of the contributors any rights or claims to the same after the code has been merged into the codebase.

Before you start, please make yourself familiar with the architecture of `rumqtt` and read the [design docs](./docs/design.md) before making your first contribution to increase it's chances of being adopted. Please follow the [Code of Conduct](./CODE_OF_CONDUCT.md) when communicating with other members of the community and keep discussions civil, we are excited to have you make your first of many contributions to this repository, welcome!
