# hakai-profile-qaqc

hakai-profile-qaqc is the main package use to handle the QAQCing of the CTD Datasets maintained by the Hakai Institute. Please refer to the [test description manual](tests_description.md) for a full description of the different tests applied within this package. Examples of tes

## Installation
The present package can be installed locally or through a docker container.
In all cases it is best to clone locally the package and apply the appropriate configuration.

```terminal 
git clone git@github.com:HakaiInstitute/hakai-profile-qaqc.git
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



### docker-compose
Create docker container and execute profile qc based on configuration parameters.
Define environment variables based on the inputs given by hakai_profiles_qc

```terminal
docker-compose up -d
```
### Caprover app
Install locally [caprover CLI service](https://caprover.com/docs/cli-commands.html).

Run the `caprover deploy` command and provide the appropriate inputs requested to execute the deployment.

To avoid restarting the process when completed the following command needs to be send to the docker container made available on the server (more details [here](https://blog.alexellis.io/containers-on-swarm/)):

```shell
sudo docker service update CONTAINER_ID --restart-condition “none"
```

## Configuration
The package configuration can be made through three different levels by order of precedence.

- environment variables: Any configuration parameters can be overwritten by similarly named environment variables.


## How To
### Run Test Suite
By default, the package will run the only the hakai test suite profiles listed within the `default-config.yaml` configuration.

### Run specific hakai_ids
You can test run on specific profiles by providing a comma separated list of hakai_ids like the following example:
```
python hakai_profile_qc --hakai_ids "080217_2017-01-08T18:03:05.167Z,080217_2017-01-26T16:56:39.000Z"
```

### QC stage specific profiles
To QC profiles associated with a specific processing stage, you generate locally through a config.yaml file or through environment variables the following 
``` yaml
RUN_TEST_SUITE: False
QC_PROCESSING_STAGES: '8_binAvg,8_rbr_processed'
UPDATE_SERVER_DATABASE: True
```
To overwrite already qced profiles, add the associated processing stages:
```yaml
QC_PROCESSING_STAGES: '8_binAvg,8_rbr_processed,9_qc_auto,10_qc_pi'
```

## CI and Deployment
This section list the different deployments and CI configurations used to maintain and deploy the present application.
### Main Branch Testing 
Any `push` or `pull requests` to the `main` branch will be tested by a linter, build and ran on the hakai profile test suite through a github workflow.

`Pull Requests` to main will also be tested through a deployment to the [caprover development app](https://captain.server.hak4i.org/#/apps/details/hakai-profile-qc-test-suite-development) which will run the the test suite from the hakaidev database.

### Automated QC of Hakai Profiles
The tool is deployed as a caprover app [here](https://captain.server.hak4i.org/#/apps/details/hakai-profile-qc-production) through a github sheduled [workflow](.github/workflows/cron-job-qc-unqced-profiles-hecate.yml) to query nightly any CTD profiles available within the Hakai CTD Data still associated with the processing stages `8_binAvg` or `8_rbr_processed`. Any of those profiles will be qced and upgraded to the `9_qc_auto` once completed.

### Automated flag tests rebuild
A full rebuild of the test flags can be performed through the following caprover apps:

#### Development database hakaidev
Rebuild is completed from https://captain.server.hak4i.org/#/apps/details/hakai-profile-qc-hakai-dev-rebuild 
and can be manually triggered by the github action workflow https://github.com/HakaiInstitute/hakai-profile-qaqc/actions/workflows/run-qc-rebuild-hakaidev-development.yml

#### Production databse hakai
Hakai production database rebuild is handled by https://captain.server.hak4i.org/#/apps/details/hakai-profile-qc-hakai-production-rebuild and can be manually triggered by the github action workflow: https://github.com/HakaiInstitute/hakai-profile-qaqc/actions/workflows/run-qc-rebuild-hakai-production.yml