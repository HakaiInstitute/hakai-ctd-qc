from hakai_qc.get import json_config, config_as_dataframe
import pandas as pd

qc_config = json_config('hakai_ctd_profile.json')
if 'hakai' in qc_config:
    hakai_config = qc_config.pop('hakai')

# Sort IOOS qc
qc_table = pd.DataFrame()
for var in qc_config.keys():
    for module in qc_config[var].keys():
        test = pd.DataFrame(qc_config[var]).stack()
        test.name = 'Inputs'
        test = test.reset_index().rename({'level_0': 'Test', 'level_1': 'Module'}, axis=1)
        test['Variable'] = var
        qc_table = qc_table.append(test)

qc_table['Inputs'] = qc_table['Inputs'].astype(str).str.replace('{','').str.replace(r', \'','<br>**').str.replace(r'\'','**').str.replace('}','')
# Make a markdown table
with open('table_qc_config.md', 'w') as f:
    f.write(qc_table.set_index(['Variable']).to_markdown())

with open('qc_config.md', 'w') as f:
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