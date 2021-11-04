# hakai-profile-qaqc
hakai-profile-qaqc is the main package use to handle the QAQCing of the Hakai Oceanography CTD Dataset.

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

# QARTOD FLAG CONVENTION
This package use the [QARTOD flagging definition](https://cdn.ioos.noaa.gov/media/2020/07/QARTOD-Data-Flags-Manual_version1.2final.pdf)
which follow the definitions presented below: 

![Alt text](QARTOD_Flag_Convetion_Table.png?raw=true "QARTOD Flag Convention")

# Summary of the tests applied
We present here a brief summary of the different tests applied. For a more accurate description of the tests and 
thresholds used, please review the [**Hakai Institute Profile QC Tests List**](doc/table_qc_config.md).

## Location test
If it is associated to a [Hakai Station](https://hakai.maps.arcgis.com/apps/webappviewer/index.html?id=38e1b1da8d16466bbe5d7c7a713d2678), 
the following tests are completed:
* Maximum profile depth do not exceed the station depth. 
    * Flag 3 if exceed by **5%** the station depth
    * Flag 4 if exceed by **10%** the station depth 
* Latitude and longitude position given is within **3km** of the station location.
* Latitude and/or longitude is unknown.

## IOOS QC tests
The following section applies a series of general tests developed and available
within the [ioos_qc tool](https://github.com/ioos/ioos_qc). You can find a each tests available and its input parameters [here](https://ioos.github.io/ioos_qc/api/ioos_qc.html#submodules).
We present here the different QARTOD tests described in the [Temperature and Salinity QARTOD Manual](https://cdn.ioos.noaa.gov/media/2017/12/qartod_temperature_salinity_manual.pdf) 
, the [Dissolved Oxygen QARTOD Manual](https://repository.oceanbestpractices.org/handle/11329/270) and their implementation as of now:

1. **Required Tests**
    1. Timing/Gap Test *[Not available]*
        * Unavailable
        * Could use instrument measurement time and compare it to start, bottom and end times. 
    1. Syntax Test *[Not available]*
        * This is completed by the Hakai Portal.
    1. Location Test
        * See section above
    1. Gross Range Test 
        * Applied for most parameters.
    1. Climatological Test
        * Not Applied 
        * Could be implemented per station, particularly Hakai's primary stations.
        
1. **Strongly Recommended**
    1. Spike Test
        * Applied only the Dissolved Oxygen Variables as of now to detect and flag communication issues observed in the past.
        * Spike test actually implemented within IOOS_QC gives some debatable results.
         Hakai will potentially suggest an alternative method of spike detection.
    1. Rate of Change Test
        * Applied only the Dissolved Oxygen Variables. 
        * As of now the rate of change test within  IOOS_QC test is using a time rate, ideally it would be better to
        use a rate based on the depth. Hakai will suggest changes to IOOS_QC rate_of_change_test to be compatible with 
        the different axis.
    1. Flat Line Test

1. **Suggested**
    1. Multi-Variate Test *[Not available]*
    1. Attenuated Signal Test
        * Used to detect if there any real data associated to a PAR, Transmissometer or Turbidity sensor.
        * Could easily be added to all other parameters.
    1. Neighbor Test *[Not available]*
    1. TS Curve/Space Test *[Not available]*
    1. Density Inversion Test. *[Added to IOOS_qc by Hakai!]*
        * This test is not yet available within the ioos_qc package. See below for Hakai's temporary implementation.

*The [ioos_qc tool](https://github.com/ioos/ioos_qc) package is as of now only compatible with time series data type.
We however did a temporary fix to make it usable with profile data. This issue should be addressed in the near future.*

## Potential Density Inversion Test (Now integrated in IOOS_QC)
We reproduce here a test similar to the Density Inversion Test applied by the ARGO program. 
*Potential density with a 0 dBar reference pressure* is derived from the Temperature, Practical Salinity and Pressure values 
with the **gsw.sigma0** method of the [TEOS-10 Python Package](https://github.com/TEOS-10/GSW-Python),
by first deriving the Absolute Salinity and Conservative Temperature. 

> [14. Density inversion](https://archimer.ifremer.fr/doc/00228/33951/32470.pdf)
>
>This test compares potential density between valid measurements in a profile, in both directions, i.e. 
>from top to bottom, and from bottom to top. Values of temperature and salinity at the same pressure level Pi
> should be used to compute potential density ρi (or σi = ρi− 1000) kg m−3, referenced to the mid-point between Pi
> and the next valid pressure level Pi+1. A threshold of 0.03 kg m−3 should be allowed for small density inversions.
>
> Action: From top to bottom, if the potential density calculated at the greater pressure is less than that 
>calculated at the lesser pressure by more than 0.03 kg m−3, both the temperature and salinity values should 
>be flagged as bad data. From bottom to top, if the potential density calculated at the lesser pressure is greater
> than that calculated at the greater pressure by more than 0.03 kg m−3, both the temperature and salinity 
>values should be flagged as bad data.

The main difference is that we're using two threshold values instead 0.03 kg m-3. A first threshold which is more 
restrictive of 0.005 kg m-3 is applied is flag as SUSPECT while a second threshold of 0.05 kg m-3 is applied and associated with a 
FAIL flag.    

Potential Density Inversion Flag is then distributed to all Temperature, Practical Salinity and Conductivity variables.

## Hakai Specific Tests
### Dissolved Oxygen Cap test
Occasionally, Hakai's field technician omit to remove the protective cap over the JFE Rinko 
dissolved oxygen sensors units, which then make the associated data unusable. In such case, the oxygen value recorded is
obviously bad and need to be flagged.  Generally, the dissolved oxygen signal recorded by the sensor present a slow
negative trend over time during the deployment which is depth independent. 
To detect this issue, we compare both the up and down cast associated with a 
profile and compute the difference in dissolved oxygen concentration recorded by each profile for a same pressure. 

If the dissolved oxygen concentration difference between the up and down cast exceed for more than 50% of the 
profile:
* &Delta;DO > 0.2 mL/L: flag as **SUSPECT** 
* &Delta;DO > 0.5 mL/L: flag as **FAIL**  
  
### Bottom Hit Detection
We use the density inversion flag to detect if an instrument hit bottom. This density inversion is likely caused by
the contact of sediments with the instrument which lowered the conductivity measured by the instrument at the same 
occasion the derived conductivity.

To be considered a bottom hit, the lowest pressure bin collected needs to be flagged with a density inversion. 
If it's the case this bin and any other consecutive bins flagged above are flagged as **FAIL**. 

This flag applies to all the variables QCed.
### PAR Boat Shadow Detection
Photosynthetically Active Radiation data is highly sensitive to the position the instrument is deployed compared to the sun. 
If deployed along the opposite side of the ship to the sun. The boat shadow will highly affect the measurement for the 
first meters near the surface. This will be represented in a PAR profile by a sudden increase in PAR values once the 
instrument gets away from the ship shadow. To detect this issue, we analyse the PAR profile from the bottom going to 
the surface and flag any data which is lower than the maximum value recorded up until this depth in the vertical profile. 

In normal conditions, we should expect PAR to follow an exponential decay with depth, going from the bottom to the surface 
this mean that a PAR profile should always be increasing as it goes closer to the surface. 
To ignore any noise effected related to lower value, we set minimum value from which the algorithm is applied. 

# How to use the package
## Installation
To install the package locally for testing or on the Hakai Servers. 

Install the Hakai Python API Client:
 ```python
pip install git+https://github.com/HakaiInstitute/hakai-api-client-python.git
```

Install the main branch of the Hakai Institute ioos_qc fork and main branch:
```python
pip install git+https://github.com/HakaiInstitute/ioos_qc@colab-compatible
```

And finally the hakai-profile-qaqc main branch:
```python
pip install git+https://github.com/HakaiInstitute/hakai-profile-qaqc.git@main
```

## Running the tool locally or on a server
To apply the test on some specific data you can use the following method you can ask the tool to run on some specific 
hakai_id by using the following command:
 ```python
json_string_out = hakai_qc.run.update_hakai_ctd_profile_data(hakai_id=['hakai_id1','hakai_id2'])
```
 You can also retrieve yourself some data through the hakai api and input them into the tool as a json list format:
 ```python
json_string_out = hakai_qc.run.update_hakai_ctd_profile_data(json_input=JSON_STRING_INPUT)
```

In both cases, the tool will ouptut a JSON string with downloaded data and the resulting flags.

