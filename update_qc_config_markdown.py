from hakai_qc.get import json_config, config_as_dataframe
import pandas as pd
import re

qc_config = json_config('hakai_ctd_profile.json')
if 'hakai' in qc_config:
    hakai_config = qc_config.pop('hakai')

def dict_to_md(dictionary):
    multiline = re.sub(r', \'', '<br>**', str(dictionary))
    multiline = re.sub(r'\':', '**:', multiline)
    multiline = re.sub(r'{\'', '{**', multiline)
    return multiline

# Sort IOOS qc
qc_table = pd.DataFrame()
for var in qc_config.keys():
    for module in qc_config[var].keys():
        test = pd.DataFrame(qc_config[var]).stack()
        test.name = 'Parameters'
        test = test.reset_index().rename({'level_0': 'Test', 'level_1': 'Module'}, axis=1)
        test['Variable'] = var
        qc_table = qc_table.append(test)

qc_table['Parameters'] = qc_table['Parameters'].astype(str).apply(dict_to_md)

# Make a markdown table
description_text = 'The following tables below present the standard parameters used by the Hakai Institute ' \
                   'to QC their CTD profile data.\n \n'

with open('table_qc_config.md', 'w') as f:
    # Add description Text
    f.write(description_text)
    # IOOS_QC tests
    for module, df_modules in qc_table.groupby('Module'):
        f.write('# ' + module.upper()+' Tests \n')
        f.write(df_modules.drop('Module', axis=1).set_index(['Variable']).to_markdown())
        f.write('\n\n')

    # Hakai Specific tests which have a slight different format
    # TODO Modify Hakai tests config to be similar to ioos_qc ones
    f.write('# HAKAI Tests \n')
    f.write('|Variable | Test| Parameters|\n')
    f.write('| :----| :----| :----|\n')
    for key in hakai_config.keys():
        if 'variable' in hakai_config[key]:
            var = hakai_config[key].pop('variable')
        f.write('|'+str(var)+'|'+key+'|'+dict_to_md(hakai_config[key])+'|\n')

