# GitHub workflows

## Build and Test (`build.yaml`)

Runs on every push and on pull requests into `main`.

- Builds `Contour.sln` in Release and runs the unit tests (`Tests/Contour.Tests`).
- Runs the RabbitMQ and Kafka integration tests in separate jobs. The brokers are started by Testcontainers.
- Packs the library to validate the package. Nothing is published.
- Fails when the source changed but `<Version>` in `Sources/Contour/Contour.csproj` is already released.
- A pull request into `main` must carry a release version (`x.y.z`).

## Publish NuGet Package (`publish.yaml`)

Runs on a push to `main` or `prerelease` and publishes the `Contour` package to the organization feed
`https://nuget.pkg.github.com/cupid-media/index.json`.

| Branch | Allowed version | Result |
|---|---|---|
| `prerelease` | `x.y.z-suffix`, for example `7.0.0-pr1` | package, tag `v<version>`, GitHub pre-release |
| `main` | `x.y.z` | package, tag `v<version>`, GitHub release |

The version is read from `<Version>` in `Sources/Contour/Contour.csproj`. The workflow never changes it:
bump it in the commit that changes the source. Publishing a version that is already in the feed fails.

To publish a pre-release: set a pre-release version in the project file and bring the commit to the
`prerelease` branch.

The package is pushed with the `NUGET_PUBLISH_TOKEN` organization secret when it is set, otherwise with the
workflow `GITHUB_TOKEN`.

## Other release lines

Contour 3.x lives in `release/3.3` with its own copy of the workflows.
