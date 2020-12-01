# Hakai CTD Profiles Known Issues 
This section present different known issues associated with the Hakai CTD Profile Dataset for which different tests 
were developed to detect and flag the problematic data.  

## Density Inversion
The following figure present an example of both issues presented below with SUSPECT flagged values related to the 
misalignment of the conductivity and temperature data and the second related to the fact the instrument hit bottom.
![Alt text](bottom_hit_and_density_inversion.png?raw=true "Density inversion example") 
### Misalignment of the Conductivity and temperature data
Small density inversions are occasionally observed within Hakai's data. Those are generally related to the use 
of a constant shift value of the temperature vs conductivity data of both Seabird and RBR CTDs. 
Those inversions are generally related to small depth ranges with constant salinity. Those density inversions are
generally smaller than 0.03 kg/m<sup>3</sup> but greater than 0.005 kg/m<sup>3</sup>.

### Instrument Bottom Hit
Greater density inversions near the bottom are generally related to the sediment or air in the conductivity cell which 
generally is related to near surface data or near the bottom when the instrument hit bottom.  

If density inversion is detected near the bottom, this is likely related to the instrument hitting the bottom. 
In those conditions, all the successive flagged bin data is also flagged as FAIL for every other 
sensors mounted on the unit. 

## Missing Instrument
On some occasion, some auxiliary sensors needs to be remove from the main CTD unit for different reasons:
1. Depth reached exceed maximum depth of the auxiliary sensor
1. Broken sensor which hasn't been replaced yet.

We detect this issue by analysing the maximum range measured during a profile by each sensor. If this measurement range
remain below an acceptable range, this data is flagged.

For some sensors (PAR), this issue is also detected based on the data given which is generally out of the 
tolerable range of instrument.

## Boat Shadow
To achieve a good PAR profile, the instrument must be deployed on the sunny side of the ship to avoid exposing the PAR 
sensor to the shadow of the ship. This is, however, rarely manageable since the currents, waves, and wind generally 
dictate the ship's heading during the completion a profile. Due to this, Hakai's PAR data is often affected by the 
shadow of the boat. 

To detect the shadow, we assume that PAR values should always be increasing as it gets to a shallower depth. If 
PAR<sub>D<sub>1</sub></sub> < PAR<sub>D<sub>2</sub></sub>, where D<sub>1</sub> < D<sub>2</sub>, the value is flagged 
as SUSPECT. To be considered in this analysis, PAR<sub>D<sub>1</sub></sub> also need to be higher than 2. 

![Alt text](par_shadow.png?raw=true "Example or PAR profile affected by a PAR shadow") 

### Dark value detection

## Loosing communication with auxiliary sensor in the profile
Hakai's SBE19 SN01907674 had intermittent issues at high depth (>250m) with it's SBE43 oxygen sensor. After further
investigation, we found out that the issue was related to the PAR sensor mounted on the unit. After all this, a 
significant amount of the Hakai data set particularly at the deeper site is affected by this issue. To over come the 
issue we recommend using the secondary oxygen sensor at high depth.      
![Alt text](do_oxygen_failure.png?raw=true "Communication Lost with DO sensor") 

## Protective Cap left on JFE Rinko Dissolved Oxygen Sensor
JFE Rinko sensors uses the optode principe to measure oxygen concentration. This type of sensor uses a sensing foil 
at the head of the unit which is sensitive to the ambient light. To reduce the drift associated to an exposure to the 
ambient light, those sensors are generally protected by a protective cap when not in used in the water. 

That protective cap is unfortunately forgotten occasionally, which is leading to bad dissolved oxygen concentration 
data. The resulting profile is generally very different than a regular profile. To detect this issue, we compare the
concentrations measured on both the upcast and downcast. If the concentration different for each respective pressure 
bin is higher than 0.2 [SUSPECT] or 0.5 [FAIL] mL/L on 50% or more of the profile, both profiles (up and down) are 
flagged.   
![Alt text](Rinko_do_cap.png?raw=true "Example of Dissolved Oxygen Profile when protective cap is 
left mounted on the sensor head") 