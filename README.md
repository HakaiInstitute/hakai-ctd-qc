# hakai-ctd-qc

`hakai-ctd-qc` is the main package used to handle the QCing of the CTD
Datasets maintained by the Hakai Institute. Please refer to the
[test description manual](tests_description.md) for a full description of the
different tests applied within this package.

## Quick Start
The following commands will start a docker container, request all ctd casts that are awaiting qc and process from the default api, and them process them in batches of `chunk_size` casts. 

```terminal
git clone git@GitHub.com:HakaiInstitute/hakai-ctd-qc.git
cd hakai-ctd-qc
cp sample.env .env
docker-compose up
```

## Installation

The present package can be installed locally or through a docker container.
In all cases, it is best to clone locally the package and apply the appropriate configuration.

```terminal
git clone git@GitHub.com:HakaiInstitute/hakai-ctd-qc.git
```

### Local development
**Requirements:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

**Install dependencies:**

```bash
uv sync
```

**Create .env:**
Copy the `sample.env` file as `.env` and replace the different values accordingly.

```bash
cp sample.env .env
```

### Methods

#### Command Line Interface


Once installed the package hakai_ctd_qc can be run via the command line.
See the help menu for a complete description of the different options:

```console
python hakai_ctd_qc --help

Usage: hakai_ctd_qc [OPTIONS]

Options:
  --hakai_ids TEXT            Comma delimited list of hakai_ids to qc
  --processing-stages TEXT    Comma list of processing_stage profiles to
                              review [env=QC_PROCESSING_STAGES]  [default:
                              8_binAvg,8_rbr_processed]
  --test-suite                Run Test suite [env=RUN_TEST_SUITE]
  --api-root TEXT             Hakai API root to use [env=HAKAI_API_ROOT]
                              [default: https://portaldev.hakai.org/api]
  --upload-flag               Update database flags
                              [env=UPDATE_SERVER_DATABASE]
  --chunksize INTEGER         Process profiles by chunk
                              [env=CTD_CAST_CHUNKSIZE]  [default: 100]
  --sentry-minimum-date TEXT  Minimum date to use to generate sentry warnings
                              [env=SENTRY_MINIMUM_DATE]
  --profile PATH              Run cProfile
  --help                      Show this message and exit.
```

#### API 

> [!IMPORTANT]
> The api code base still exists but is not accessible in production deployments. These instructions are left here for reference only

Run the following command:

```
uv run python hakai_ctd_qc/api.py
```

And within a browser to go: <http://127.0.0.1:8000>

With vscode you can also run the debug configuration `Run API` which helps debug the interface in realtime.

> [!NOTE]
> To protect the api from unexpected calls, you can set a list of accepted tokens as a list of comma separated list.
> Any post calls to the api will then require a `token` field within the header of the post command and an accepted value.

### Deployments

The hakai_ctd_qc tool is deployed via a Docker container (see [Dockerfile](Dockerfile)) and run from Windmill. On container start, the application
will request all ctd casts that are awaiting qc and process them in batches of `chunk_size` casts. 

- development: https://windmill-dev-server.windmill.hakai.app/scripts/get/d35488d8aec4898b?workspace=data-pipelines -> qc hakaidev database
- main: https://windmill-dev-server.windmill.hakai.app/scripts/get/58d138bc6d80e3d4?workspace=data-pipelines -> qc hakai database
    - a cron schedule is applied to this instance to qc latest data submitted.

see [Windmill Schedules](https://windmill-dev-server.windmill.hakai.app/schedules?filter_kind=schedule&user_and_folders_only=false&status=all) for container run schedule

### Continuous Integration

1. **Testing**: Any changes to the package are tested via a [GitHub workflow](.github/workflows/test-package-install.yml) that runs the hakai_id test suite.
2. **Docker Build Testing**: Docker container build is tested via a [GitHub workflow](.github/workflows/test-docker-build.yml).
3. Changes to the main and development versions trigger image builds which are in turn pulled into windmill on next scheduled run. [prod deploy action](.github/workflows/build.yml) [dev deploy action](.github/workflows/build_dev.yml).
4. **Errors and monitoring**: Sentry is used to monitor the different errors and cron jobs. Only the main deployment is required to run a cron job to make sure any newly submitted data is QCed. See the following links for any [issues](https://hakai-institute.sentry.io/projects/ctd-auto-qc/?project=6685251) and [cron issues](https://hakai-institute.sentry.io/crons/8ac7c3da-4e18-4c7b-9ce9-c0fa22956775/?project=6685251&statsPeriod=7d) encountered.

### Tests parametrization

The different tests applied are defined within the respective configurations:

- [Hakai tests](hakai_ctd_qc/config/hakai_ctd_profile_tests_config.json)
- [QARTOD Tests](hakai_ctd_qc/config/hakai_ctd_profile_qartod_test_config.json)

A subset of hakai_ids is used to test the qc tool and is maintained [here](hakai_ctd_qc/config/HAKAI_ID_TEST_SUITE.txt)

Manual flags can also be implemented on any instrument-specific variables via the [grey-list](hakai_ctd_qc/HakaiProfileDatasetGreyList.csv),
which overwrites any automatically generated flags.

### Testing

To make sure the tests are working appropriately a series of pytests are available. Some of the tests are specific to the hakai tests, others to the hakai test suite. 

The test suite is made available locally via the parquet file, or retrieved from the development or production database.

To run all the tests locally:

```shell
uv run pytest .
```

To run all the tests with the production data (hecate) or development data (goose). Use the `--test-suite-from` option. Here's an example for goose:

```shell
uv run pytest . --test-suite-from goose
```

Once to test the results on any of the databases without rerunning the tests on the data, you can use the `--test-suite-qc False` option.

```shell
uv run pytest . --test-suite-form goose -k test_source_expected_results
```
