# hakai-profile-qaqc
Series of tests applied to the Hakai CTD profile data based on the QARTOD tests and other Hakai specific ones.  

You can run the different tests on your own through the following link:
https://colab.research.google.com/github/HakaiInstitute/hakai-profile-qaqc/blob/main/review-hakai-tests.ipynb

# QARTOD FLAG CONVENTION
This package use the [QARTOD flagging definition](https://cdn.ioos.noaa.gov/media/2020/07/QARTOD-Data-Flags-Manual_version1.2final.pdf)
which follow the definitions presented below: 

![Alt text](QARTOD_Flag_Convetion_Table.png?raw=true "QARTOD Flag Convention")

# Summary of the tests applied
We present here a brief summary of the different tests applied. 

## Location test
If it is associated to a [Hakai Station](https://hakai.maps.arcgis.com/apps/webappviewer/index.html?id=38e1b1da8d16466bbe5d7c7a713d2678), 
the following tests are completed:
* Maximum profile depth do not exceed the station depth. 
    * Flag 3 if exceed by **5%** the station depth
    * Flag 4 if exceed by **10%** the station depth 
* Latitude and longitude position given is within **3km** of the station location.

## IOOS QC tests
The following section applies a series of general tests developed and available
within the [ioos_qc tool](https://github.com/ioos/ioos_qc). You can find a each tests available and its input parameters [here](https://ioos.github.io/ioos_qc/api/ioos_qc.html#submodules).
We present here the different QARTOD tests described in the [Temperature and Salinity QARTOD Manual](https://cdn.ioos.noaa.gov/media/2017/12/qartod_temperature_salinity_manual.pdf) 
and their implementation as of now:

1. **Required Tests**
    1. Timing/Gap Test *[Not available]*
        * Unavailable
        * Could use instrument measurement time and compare it to start, bottom and end times. 
    1. Syntax Test *[Not available]*
        * This is completed by the Hakai Portal.
    1. Location Test
        * See section above
    1. Gross Range Test
        * As of now, it is applied to the follow parameters: Depth, Pressure, Salinity, Temperature, Dissolved_Oxygen_ml_l, Rinko_do_ml_l, Turbidity, PAR, Fluorescence.
    1. Climatological Test
        * Not Applied 
        * Could be implemented per station, particularly Hakai's primary stations.
        
1. **Strongly Recommended**
    1. Spike Test
        * Applied only the Dissolved Oxygen Variables as of now to detect and flag communication issues observed in the past.
    1. Rate of Change Test
        * Applied only the Dissolved Oxygen Variables
    1. Flag Line Test

1. **Suggested**
    1. Multi-Variate Test *[Not available]*
    1. Attenuated Signal Test
        * Used to detect if there any real data associated to a PAR, Transmissometer or Turbidity sensor.
    1. Neighbor Test *[Not available]*
    1. TS Curve/Space Test *[Not available]*
    1. Density Inversion Test. *[Not available]*
        * This test is not yet available within the ioos_qc package. See below for Hakai's temporary implementation.


*The [ioos_qc tool](https://github.com/ioos/ioos_qc) package is as of now only compatible with time series data type.
We however did a temporary fix to make it usable with profile data. This is issue should be address in the near future.*