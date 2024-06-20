# hakai-profile-qaqc

hakai-profile-qaqc is the main package use to handle the QAQCing of the CTD Datasets maintained by the Hakai Institute. Please refer to the [test description manual](tests_description.md) for a full description of the different tests applied within this package. Examples of tes

## Installation

The present package can be installed locally or through a docker container.
In all cases it is best to clone locally the package and apply the appropriate configuration.

```terminal
git clone git@GitHub.com:HakaiInstitute/hakai-profile-qaqc.git
```

### Locally for development only

Clone locally the repository and create the conda environment:

```terminal
pyenv install 3.11.2
pyenv local 3.11.2
pip install poetry
poetry run python hakai_profile_qc
```

Run python tool directely:

```terminal
poetry run pyton hakai_profile_qc
```

### How to

Once installed the package hakai_profile_qc can be run via the command line. See help menu for a complete description of the different options:

```console
python hakai_profile_qc --help

Usage: hakai_profile_qc [OPTIONS]

  QC Hakai Profiles on subset list of profiles given either via an  hakai_id
  list, the `test_suite` flag or processing_stage.  If no input is given, the
  tool will default to qc all the profiles  that have been processed but not
  qced yet:      processing_stage={8_binAvg,8_rbr_processed}

  Each options can be defined either as an argument  or via the associated
  environment variable.

Options:
  --hakai_ids TEXT                Comma delimited list of hakai_ids to qc
  --processing-stages TEXT        Comma list of processing_stage profiles to
                                  review [env=QC_PROCESSING_STAGES]  [default:
                                  8_binAvg,8_rbr_processed]
  --test-suite                    Run Test suite [env=RUN_TEST_SUITE]
  --api-root TEXT                 Hakai API root to use
                                  [env=HAKAI_API_SERVER_ROOT]  [default:
                                  https://goose.hakai.org/api]
  --upload-flag                   Update database flags
                                  [env=UPDATE_SERVER_DATABASE]
  --chunksize INTEGER             Process profiles by chunk
                                  [env=CTD_CAST_CHUNKSIZE]  [default: 100]
  --sentry-minimum-date [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]
                                  Minimum date to use to generate sentry
                                  warnings [env=SENTRY_MINIMUM_DATE]
  --profile PATH                  Run cProfile
  --help                          Show this message and exit.
```

### Deployments

The hakai_profile_qc tool is deployed in multiple ways a Docker containers. See [Dockerfile](Dockerfile).

1. **Testing**: Any changes to the package is tested via a [GitHub workflow](.GitHub/workflows/test-package-install.yml) that qc hakai_id test suite.
2. **Docker Build Testing**: Docker container build is tested via a [GitHub worflow](.GitHub/workflows/test-docker-build.yml)
3. **hakaidev db rebuild**: A full rebuild of the hakaidev processed ctd data can be triggered via the [GitHub worflow](.GitHub/workflows/run-qc-rebuild-hakaidev-development.yml). This will trigger a new run at <https://captain.server.hak4i.org/#/apps/details/hakai-profile-qc-hakai-development-rebuild>
4. **hakai db rebuild**: A full rebuild of the hakaidev processed ctd data can be triggered via the [GitHub worflow](.GitHub/workflows/run-qc-rebuild-hakaidev-production.yml). This will trigger a new run at <https://captain.server.hak4i.org/#/apps/details/hakai-profile-qc-hakai-production-rebuild>
5. **QC latest profiles cronjob**: An instance is deployed on hecate.hakai.org via the cronjob and is triggered nightly to qc the latest profiles. This instance is monitored via sentry at for any [issues](https://hakai-institute.sentry.io/projects/ctd-auto-qc/?project=6685251) and [cron issues](https://hakai-institute.sentry.io/crons/8ac7c3da-4e18-4c7b-9ce9-c0fa22956775/?project=6685251&statsPeriod=7d)

### Tests parametrization

The different tests applied are defined within the respective configurations:

- [Hakai tests](hakai_profile_qc/config/hakai_ctd_profile_tests_config.json)
- [QARTOD Tests](hakai_profile_qc/config/hakai_ctd_profile_qartod_test_config.json)

A subset of hakai_ids is used to test the qc tool and is maintained [here](hakai_profile_qc/config/HAKAI_ID_TEST_SUITE.txt)

Manual flags can also be impleted on any instrument specific variables via the [grey-list](hakai_profile_qc/HakaiProfileDatasetGreyList.csv) which overwrites any automatically generated flags.
