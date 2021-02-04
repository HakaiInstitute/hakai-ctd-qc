import hakai_qc
from hakai_api import Client

# Use hakai_id list method
hakai_id_list = ['080217_2017-01-08T18:03:05.167Z', '01907674_2016-10-12T16:23:36Z', '080217_2015-09-10T23:07:01.167Z']
# 080217_2015-09-10T23:07:01.167Z has a broken do and fluo sensor test for grey list

json_output_from_hakai_id_method = hakai_qc.run.update_hakai_ctd_profile_data(hakai_id=hakai_id_list)

# Use JSON input method
variable_lists = hakai_qc.get.hakai_api_selected_variables()
[json_input, url] = hakai_qc.get.hakai_ctd_data('hakai_id={' + ','.join(hakai_id_list) + '}' +
                                                '&limit=-1' +
                                                '&fields=' + ','.join(variable_lists)
                                                , output_format='json')
json_output_from_json_method = hakai_qc.run.update_hakai_ctd_profile_data(json_input=json_input)
# TODO need to review both inputs they should be outputting the same thing

# Compare both methods
if json_output_from_hakai_id_method == json_output_from_json_method:
    print('SUCCESS!')