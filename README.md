# hakai-profile-qaqc
hakai-profile-qaqc is the main package use to handle the QAQCing of the Hakai Oceanography CTD Dataset. Please refer to the [test description manual](tests_description.md) for a full description of the different tests applied within this package.

## Installation
Run the following command in a command line

```
pip install git+git://github.com/HakaiInstitute/hakai-profile-qaqc.git
```

## How To

To run test on an individual profile. Run the following command for a comma separated list of hakai IDs:
```
python3 -m hakai_profile_qc.review -hakai_id 080217_2017-01-08T18:03:05.167Z,080217_2017-01-26T16:56:39.000Z
```

Run the following command for a specific station:
```
python3 -m hakai_profile_qc.review -station QU39
```
You can also use the following Jupyter Notebooks:
- https://colab.research.google.com/github/HakaiInstitute/hakai-profile-qaqc/blob/main/notebook/review-hakai-tests.ipynb
- https://colab.research.google.com/github/HakaiInstitute/hakai-profile-qaqc/blob/main/notebook/generate-research-ncfiles.ipynb

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

![Alt text](QARTOD_Flag_Convetion_Table.png?raw=true "QARTOD Flag Convention")