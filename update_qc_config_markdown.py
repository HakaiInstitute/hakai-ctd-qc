from hakai_qc.get import json_config


qc_config = json_config('hakai_ctd_profile.json')
with open('qc_config.md', 'w') as f:
    if 'hakai' in qc_config:
        hakai_config = qc_config.pop('hakai')

    # IOOS_QC tests
    f.write('#  IOOS QC TESTS \n')
    for var in qc_config:
        f.write('## ' + var + '\n')
        for module in qc_config[var]:
            f.write('### ' + module + '\n')
            for test in qc_config[var][module]:
                f.write('* ' + test + '\n')
                for input in qc_config[var][module][test]:
                    f.write('   * ' + input + ': ' + str(qc_config[var][module][test][input]) + '\n')

    # Hakai Specific tests
    f.write('#  Hakai QC TESTS \n')
    for test in hakai_config:
        f.write('## ' + test + '\n')
        for input in hakai_config[test]:
            f.write('   * ' + input + ': ' + str(hakai_config[test][input]) + '\n')