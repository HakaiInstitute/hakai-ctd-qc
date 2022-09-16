# hakai-profile-qaqc

hakai-profile-qaqc is the main package use to handle the QAQCing of the CTD Datasets maintained by the Hakai Institute. Please refer to the [test description manual](tests_description.md) for a full description of the different tests applied within this package. Examples of tes

## Install and configure

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

To update flags on the development server

```
python hakai_profile_qc --run_test_suite --config_kwargs "{'HAKAI_API_SERVER_ROOT':'https://goose.hakai.org/api', 'UPDATE_SERVER_DATABASE':true}"
```

## Production

The tool can be run on the production server and ran on the unqced profiles by using the following command line:

```
python hakai_profile_qc --qc_unqced_profiles --config_kwargs "{'HAKAI_API_SERVER_ROOT':'https://hecate.hakai.org/api', 'UPDATE_SERVER_DATABASE':true}"
```

## Tests applied

Series of tests applied to the Hakai CTD profile data based on the QARTOD tests and other Hakai specific ones.
A full list of the different tests applied now is available in the
[**Hakai Institute Profile QC Tests List**](doc/table_qc_config.md)

It is also possible to run the different tests on your own through an online Jupyter Notebook:
https://colab.research.google.com/github/HakaiInstitute/hakai-profile-qaqc/blob/main/review-hakai-tests.ipynb

## Generate NetCDF datasets

To generate the source files for the Hakai Research Dataset, you can use the following jupyter notebook here:
https://colab.research.google.com/github/HakaiInstitute/hakai-profile-qaqc/blob/main/generate-research-ncfiles.ipynb

## FLAG CONVENTION

This package use the [QARTOD flagging definition](https://cdn.ioos.noaa.gov/media/2020/07/QARTOD-Data-Flags-Manual_version1.2final.pdf)
which follow the definitions presented below:

![Alt text](QARTOD_Flag_Convetion_Table.png?raw=true 'QARTOD Flag Convention')

```

```
