import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

import folium
import folium.plugins as plugins

import ipywidgets as widgets
from ipywidgets import interact, interact_manual

import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import seaborn as sns


def test_results_bar_plot(df,
                          test_list='_flag$|_test$',
                          profile_id='hakai_id',
                          direction_flag='direction_flag'):
    # Get the flag columns and hakai_id, but ignore the direction flag column
    if type(test_list) is str:
        test_list = sorted(df.filter(regex=test_list).columns.tolist())

    test_list.append(profile_id)
    test_list.remove(direction_flag)

    # Get Flag columns, stack them all and add a count column
    df_test = df[test_list].set_index(profile_id).stack().reset_index().rename({'level_1': 'Test', 0: 'Flag'}, axis=1)
    df_test['Count'] = 1

    # Regroup data by test and flag and count how many values there is per flag
    df_result = df_test.groupby(['Test', 'Flag'])['Count'].count().dropna().reset_index()

    # Make sure that all the flags are in int and convert to str format for plotly, sort by flag and test
    #  map Hakai AV, SVC and SVD to QARTOD flags
    if df_result['Flag'].dtype is object:  # Convert Hakai flags that already exist in the database to QARTOD equivalent
        df_result['Flag'].replace({'AV': 1, 'SVC': 3, 'SVD': 4}, inplace=True)

    df_result['Flag'] = df_result['Flag'].astype(int).astype(str)
    df_result = df_result.sort_values(['Flag', 'Test'])

    # Create bar plot
    fig = px.bar(df_result, x='Count', y='Test', orientation='h', color='Flag',
                 color_discrete_sequence=['green', 'yellow', 'orange', 'red', 'purple'])
    fig.update_layout(autosize=False, width=950, height=1000)
    fig.update_yaxes(dtick=1)
    return fig


def interactive_profile_viewer(df,
                               variable_list,
                               test_list,
                               var_to_plot=None):
    if var_to_plot is None:
        var_to_plot = variable_list

    qartod_color = {1: 'green', 2: 'yellow', 3: 'orange', 4: 'red', 9: 'purple', '1': 'green', '2': 'yellow',
                    '3': 'orange', '4': 'red', '9': 'purple'}
    dir = {'d': 'downcast', 'u': 'upcast'}

    print('Select Flagged Variable(s) to consider in the following list:')

    @interact
    def which_profiles_to_look_at(flag_type=widgets.Dropdown(options=test_list, value='None',
                                                             description='Test to review', disabled=False),
                                  considered_flag=widgets.SelectMultiple(options=[1, 2, 3, 4, 9],
                                                                         value=[1, 2, 3, 4, 9],
                                                                         description='Flag To Consider',
                                                                         disabled=False),
                                  consider_only_downcast=True):

        if consider_only_downcast:
            df_selected = df[df['direction_flag'] == 'd']
        else:
            df_selected = df

        if flag_type == 'None':
            hakai_id_list = df_selected['hakai_id'].unique().tolist()
        else:
            hakai_id_list = df_selected[df_selected[flag_type].isin(considered_flag)]['hakai_id'].unique()
        print(str(len(hakai_id_list)) + ' profiles are available')

        if len(hakai_id_list)>0:
            @interact
            def plot_profile(hakai_id=hakai_id_list,
                             ocean_variables=widgets.SelectMultiple(options=variable_list - {'depth', 'pressure'},
                                                                    value=var_to_plot,
                                                                    description='Ocean Variable',
                                                                    disabled=False),
                             y_axis=widgets.Dropdown(options=variable_list, value='depth', description='Y Axis Variable',
                                                     disabled=False),
                             downcast=widgets.Checkbox(value=True, description='Downcast',
                                                       disabled=False),
                             upcast=widgets.Checkbox(value=True, description='Upcast',
                                                     disabled=False),
                             par_log_scale=widgets.Checkbox(value=True, description='PAR Log Scale',
                                                            disabled=False)):

                cast_direction = []
                if downcast:
                    cast_direction.append('d')
                if upcast:
                    cast_direction.append('u')

                # Get hakai_id data
                df_temp = df[df['hakai_id'] == hakai_id].sort_values(['direction_flag', 'depth'])

                # Create Subplots
                fig = make_subplots(rows=1, cols=len(ocean_variables), shared_yaxes=True,
                                    horizontal_spacing=0.01)
                kk = 1
                for var in ocean_variables:
                    for direction_flag in cast_direction:
                        for flag, color in qartod_color.items():
                            df_flag = df_temp[
                                (df_temp[var + '_qartod_flag'] == flag) & (df_temp['direction_flag'] == direction_flag)]

                            if len(df_flag):
                                if direction_flag is 'u':
                                    marker_dict = dict(color=color, symbol='x')
                                else:
                                    marker_dict = dict(color=color)

                                fig.add_trace(
                                    go.Scatter(x=df_flag[var],
                                               y=df_flag[y_axis],
                                               mode='markers',
                                               marker=marker_dict,  # df_temp[var+'_qartod_flag'],
                                               text=df_flag[var + '_flag_description'],
                                               name=var + ' ' + dir[direction_flag] + ' FLAG:' + str(flag)),
                                    row=1, col=kk)

                    if var in ['par'] and par_log_scale:  # Make PAR x axis log
                        fig.update_xaxes(type="log", row=1, col=kk)
                    fig.update_xaxes(title=var, row=1, col=kk)
                    kk = kk + 1

                # Add stuff around each figures
                fig.update_yaxes(title_text=y_axis, row=1, col=1)
                fig.update_yaxes(autorange="reversed", linecolor='black', mirror=True, ticks='outside', showline=True)
                fig.update_xaxes(mirror=True, ticks='outside', showline=True, tickangle=45, linecolor='black')
                fig.update_layout(height=800, width=2000, showlegend=True,
                                  title_text='Hakai ID: ' + hakai_id + ' Site: ' + df_temp['station'].unique()[0])
                print(hakai_id)
                return fig.show()

            return
        else:
            print('No Profile Available')
    return


def flag_result_map(df,
                    flag_variable='position_qartod_flag',
                    groupby_var='hakai_id'):
    # Start the map with center on the average lat/long
    center_map = df.groupby(groupby_var)[['latitude', 'longitude']].mean().mean().to_list()

    # Start by defining the map
    m = folium.Map(
        location=center_map,
        zoom_start=9, control_scale=True,
        tiles='Stamen Terrain')
    # Create groups
    fg = folium.FeatureGroup('QARTOD FLAG')

    # Add each flagged profiles grouped by group variable (default: hakai_id) as a separate icon on the map
    # SUSPECT PROFILES
    f3 = folium.plugins.FeatureGroupSubGroup(fg, 'SUSPECT')
    for index, row in df[df[flag_variable] == 3].groupby(by=groupby_var):
        f3.add_child(
            folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[SUSPECT] hakai_id: ' + str(index),
                          icon=folium.Icon(color='orange', icon='question-sign')))
    # FAIL PROFILES
    f4 = folium.plugins.FeatureGroupSubGroup(fg, 'FAIL')
    for index, row in df[df[flag_variable] == 4].groupby(by=groupby_var):
        f4.add_child(
            folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[FAIL] hakai_id: ' + str(index),
                          icon=folium.Icon(color='red', icon='question-sign')))
    # UNKNOWN
    f9 = folium.plugins.FeatureGroupSubGroup(fg, 'UNKNOWN')
    for index, row in df[df[flag_variable] == 9].groupby(by=groupby_var):
        f9.add_child(
            folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[UNKNOWN] hakai_id: ' + str(index),
                          icon=folium.Icon(color='purple', icon='question-sign')))

    # All the ones that succeed can just be a fast marker cluster
    f1 = folium.plugins.FeatureGroupSubGroup(fg, 'GOOD')
    f1.add_child(folium.plugins.FastMarkerCluster(df[df[flag_variable] == 1]
                                                  .groupby(by=groupby_var).first()
                                                  [['latitude', 'longitude']].values))

    m.add_child(fg)
    m.add_child(f1)
    m.add_child(f3)
    m.add_child(f4)
    m.add_child(f9)
    folium.LayerControl().add_to(m)
    return m


def flag_result_plot(df,
                      variables_to_plot,
                      hakai_id_to_plot,
                      y_axis_var='depth',
                      flag_type='_qartod_aggregate'):

    # Palette color for flag
    color_dict = {1: 'seagreen', 2: 'gray', 3: 'darkorange', 4: 'red', 9: 'purple'}

    # Define legend
    legend_elements = [
        mlines.Line2D([], [], color=color_dict[1], marker='s', markersize=10, linestyle='None', label='GOOD'),
        mlines.Line2D([], [], color=color_dict[3], marker='s', markersize=10, linestyle='None', label='SUSPECT'),
        mlines.Line2D([], [], color=color_dict[4], marker='s', markersize=10, linestyle='None', label='BAD'),
        mlines.Line2D([], [], color='black', marker='.', markersize=10, linestyle='None', label='Down Cast'),
        mlines.Line2D([], [], color='black', marker='x', markersize=7, linestyle='None', label='Up Cast')]

    # Loop  through each profiles and variable and create plot
    for hakai_id in hakai_id_to_plot:
        print(hakai_id)
        plt.figure()
        fig, axs = plt.subplots(1, len(variables_to_plot),
                                sharex=False, sharey=True)
        fig.set_figwidth(4 * len(variables_to_plot))
        fig.set_figheight(10)
        fig.suptitle('Hakai ID: ' + hakai_id)

        axs[0].invert_yaxis()

        kk = 0
        for variable in variables_to_plot:
            g = sns.scatterplot(data=df[df['hakai_id'] == hakai_id],
                                x=variable, y=y_axis_var,
                                hue=variable + flag_type,
                                palette=color_dict,
                                style='direction_flag',
                                linewidth=0, ax=axs[kk], legend=False)
            kk = kk + 1
        plt.subplots_adjust(wspace=0, hspace=0)

        plt.legend(handles=legend_elements,
                   bbox_to_anchor=(1, 1.02),
                   loc='lower right', ncol=2, borderaxespad=0.)

    return fig

