# hakai-profile-qaqc

hakai-profile-qaqc is the main package use to handle the QAQCing of the CTD Datasets maintained by the Hakai Institute. Please refer to the [test description manual](tests_description.md) for a full description of the different tests applied within this package. Examples of tes

## Install and configure
The present package can be installed locally or through a docker container.
In all cases it is best to clone locally the package and apply the appropriate configuration.

```terminal 
git clone git@github.com:HakaiInstitute/hakai-profile-qaqc.git
```
### Configuration
The package configuration can be made through three different levels by order of precedence.

- default-config.yaml: default configuration file
- config.yaml (optional): configuration
- environment variables: Only the following variables are retrieved if available:
    - HAKAI_API_TOKEN: Token pass to the hakai api service
    - ENVIRONMENT: environment used in sentry to track the different deployments
    - HAKAI_API_SERVER_ROOT: API root to interact with to retrieve and upload data.
    - UPDATE_SERVER_DATABASE: bool to upload resulting flags on server

### Development 
Create the conda environment:

```terminal
conda env create -f environment.yaml
```

Activate the environment

```terminal
conda activate hakai_qc
```

The package relies on the configuration file available under hakai_profile_qc/config/configl.yaml. You can potentially modify the configuration file or pass in any changes to the configuration as a json string dictionary as an input parameer to the command line option
`--config_kwargs``

### docker-compose
The processing of the latest not quality controlled ctd profiles can be executed through a docker-compose command. Use the following command to run the docker container:
```terminal
docker-compose up -d
```

## How To

To run test on an individual profile. Run the following command for a comma separated list of hakai IDs:

```
python hakai_profile_qc --qc_profiles_filter "hakai_id={080217_2017-01-08T18:03:05.167Z,080217_2017-01-26T16:56:39.000Z}"

To process the latest unprocessed profiles
```

### Development

Hakai's test suite profiles can be run quickly through the goose sever by using the following command line.

```
python hakai_profile_qc --run_test_suite
```

By default the data from the hakaidev database will be download through the goose.hakai.org/api.


## Production

The tool can be run on the production server and ran on the unqced profiles by using the following command line with the right configuration setup:

```
python hakai_profile_qc --qc_unqced_profiles
```
## Deployment
### Batch processing

The tool is deployed as an app [here](https://captain.server.hak4i.org/#/apps/details/hakai-profile-qc-batch-mode) to be run on irregular bassis to process a significant amount of the profiles without taking over any of the ressources of the production sever. 

To avoid restarting the process on completed the following command needs to be send to the docker container made available on the server (more details [here](https://blog.alexellis.io/containers-on-swarm/)):

```shell
sudo docker service update CONTAINER_ID --restart-condition “none"
```

### Regular drop QC 

A cron job on the hecate.hakai.org server is used to qc the latest submitted profiles.

## Tests applied

Series of tests applied to the Hakai CTD profile data based on the QARTOD tests and other Hakai specific ones.
A full list of the different tests applied now is available in the
[**Hakai Institute Profile QC Tests List**](doc/table_qc_config.md)

It is also possible to run the different tests on your own through an online Jupyter Notebook:
https://colab.research.google.com/github/HakaiInstitute/hakai-profile-qaqc/blob/main/review-hakai-tests.ipynb

<!-- ## Generate NetCDF datasets

To generate the source files for the Hakai Research Dataset, you can use the following jupyter notebook here:
https://colab.research.google.com/github/HakaiInstitute/hakai-profile-qaqc/blob/main/generate-research-ncfiles.ipynb -->

## FLAG CONVENTION

This package use the [QARTOD flagging definition](https://cdn.ioos.noaa.gov/media/2020/07/QARTOD-Data-Flags-Manual_version1.2final.pdf)
which follow the definitions presented below:

![Alt text](QARTOD_Flag_Convention_Table.png?raw=true 'QARTOD Flag Convention')

```

```
