The following tables below present the standard parameters used by the Hakai Institute to QC their CTD profile data.
 
# HAKAI Tests 
| Variable    | Test                 | Parameters                                                                                                                                                                                                      |
|:------------|:---------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hakai_tests | bad_value_test       | {**variable**: 'all'<br>**flag_list**: ['.isna', -9.99e-29]}                                                                                                                                                    |
| hakai_tests | bottom_hit_detection | {**variable**: 'sigma0_qartod_density_inversion_test'<br>**profile_direction_variable**: 'direction_flag'}                                                                                                      |
| hakai_tests | do_cap_test          | {**variable**: ['dissolved_oxygen_ml_l'<br>**rinko_do_ml_l']<br>**bin_size**: 1<br>**suspect_threshold**: 0.2<br>**fail_threshold**: 0.5<br>**ratio_above_threshold**: 0.5<br>**minimum_bins_per_profile**: 10} |
| hakai_tests | par_shadow_test      | {**variable**: 'par'<br>**min_par_for_shadow_detection**: 5}                                                                                                                                                    |

# QARTOD Tests 
| Variable                 | Test                   | Parameters                                                                                                                                |
|:-------------------------|:-----------------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| position                 | location_test          | {**bbox**: [-180, -90, 180, 90]<br>**target_range**: 3000}                                                                                |
| pressure                 | gross_range_test       | {**suspect_span**: [0, 12000]<br>**fail_span**: [0, 12000]<br>**maximum_suspect_depth_ratio**: 1.05<br>**maximum_fail_depth_ratio**: 1.1} |
| depth                    | gross_range_test       | {**suspect_span**: [0, 12000]<br>**fail_span**: [0, 12000]<br>**maximum_suspect_depth_ratio**: 1.05<br>**maximum_fail_depth_ratio**: 1.1} |
| dissolved_oxygen_ml_l    | attenuated_signal_test | {**suspect_threshold**: 0.1<br>**fail_threshold**: 0.01<br>**check_type**: 'range'}                                                       |
| dissolved_oxygen_ml_l    | gross_range_test       | {**fail_span**: [0, 20]<br>**suspect_span**: [1, 15]}                                                                                     |
| dissolved_oxygen_ml_l    | rate_of_change_test    | {**threshold**: 3}                                                                                                                        |
| dissolved_oxygen_ml_l    | spike_test             | {**suspect_threshold**: 0.5<br>**fail_threshold**: 1}                                                                                     |
| dissolved_oxygen_percent | attenuated_signal_test | {**suspect_threshold**: 0.1<br>**fail_threshold**: 0.01<br>**check_type**: 'range'}                                                       |
| dissolved_oxygen_percent | gross_range_test       | {**fail_span**: [-1, 150]<br>**suspect_span**: [0, 140]}                                                                                  |
| dissolved_oxygen_percent | rate_of_change_test    | {**threshold**: 30}                                                                                                                       |
| dissolved_oxygen_percent | spike_test             | {**suspect_threshold**: 20<br>**fail_threshold**: 40}                                                                                     |
| rinko_do_ml_l            | attenuated_signal_test | {**suspect_threshold**: 0.1<br>**fail_threshold**: 0.01<br>**check_type**: 'range'}                                                       |
| rinko_do_ml_l            | gross_range_test       | {**fail_span**: [0, 20]<br>**suspect_span**: [1, 15]}                                                                                     |
| rinko_do_ml_l            | rate_of_change_test    | {**threshold**: 3}                                                                                                                        |
| rinko_do_ml_l            | spike_test             | {**suspect_threshold**: 0.5<br>**fail_threshold**: 1}                                                                                     |
| turbidity                | attenuated_signal_test | {**suspect_threshold**: 0.01<br>**fail_threshold**: 0.001<br>**check_type**: 'range'}                                                     |
| turbidity                | gross_range_test       | {**fail_span**: [-0.1, 10000]<br>**suspect_span**: [0, 1000]}                                                                             |
| c_star_at                | attenuated_signal_test | {**suspect_threshold**: 0.002<br>**fail_threshold**: 0.0001<br>**check_type**: 'range'}                                                   |
| c_star_at                | spike_test             | {**suspect_threshold**: 0.5<br>**fail_threshold**: 1}                                                                                     |
| par                      | attenuated_signal_test | {**suspect_threshold**: 0.05<br>**fail_threshold**: 0.02<br>**check_type**: 'std'<br>**min_obs**: 5}                                      |
| par                      | gross_range_test       | {**fail_span**: [-1, 100000]<br>**suspect_span**: [-0.5, 50000]}                                                                          |
| salinity                 | gross_range_test       | {**fail_span**: [0, 45]<br>**suspect_span**: [2, 42]}                                                                                     |
| salinity                 | rate_of_change_test    | {**threshold**: 5}                                                                                                                        |
| salinity                 | spike_test             | {**suspect_threshold**: 0.5<br>**fail_threshold**: 1}                                                                                     |
| temperature              | gross_range_test       | {**fail_span**: [-2, 100]<br>**suspect_span**: [-2, 40]}                                                                                  |
| temperature              | rate_of_change_test    | {**threshold**: 5}                                                                                                                        |
| conductivity             | gross_range_test       | {**fail_span**: [-0.1, 100]<br>**suspect_span**: [0, 100]}                                                                                |
| sigma0                   | density_inversion_test | {**suspect_threshold**: -0.005<br>**fail_threshold**: -0.03}                                                                              |
| flc                      | gross_range_test       | {**fail_span**: [-0.5, 150]<br>**suspect_span**: [-0.1, 80]}                                                                              |

