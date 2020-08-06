

# #0.0 Housekeeping. Clear variable space
# from IPython import get_ipython  #run magic commands
# ipython = get_ipython()
# ipython.magic("reset -f")
# ipython = get_ipython()

import pandas as pd, geopandas as gpd, fiona, os, inflection, matplotlib.pyplot as plt, pyepsg, folium, math
from folium import plugins

path_to_data = r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Passive Projects' \
               r'\Freeval-PA\grade_data\June_23_2020'
path_to_grade_data_file = os.path.join(path_to_data, 'Processing.gdb')
fiona.listlayers(path_to_grade_data_file)

# gpdfreeval_dat = gpd.read_file(filename=path_to_grade_data_file, layer='FREEVAL_80_Butler')
# freeval_grade_penndot = gpd.read_file(filename=path_to_grade_data_file, layer='FREEVAL_GRADE_PENNDOT')
# spat_join_eb_dat = gpd.read_file(filename=path_to_grade_data_file, layer='SpatialJoin80EB')
spat_join_eb_tot_dat = gpd.read_file(filename=path_to_grade_data_file, layer='SpatialJoinTotal80EB')
# spat_join_eb_tot_dat_temp = pd.DataFrame(spat_join_eb_tot_dat.drop(columns='geometry'))
temp = gpd.read_file(filename=path_to_grade_data_file, layer='SpaitialJoinTotal80EBTable')

# read data
def read_data(filename_gdf, layer_gdf, sort_fkey_asc=True):
    gdf = gpd.read_file(filename=filename_gdf, layer=layer_gdf)
    gdf.columns = [inflection.underscore(colname) for colname in gdf.columns]
    subset_gdf = gdf[['name','juris','cty_code','st_rt_no','seg_bgn','seg_end','seg_no',
                      'side_ind','seg_lngth_','seg_lngth_2','seq_no','fkey', 'flength','foffset','seg_bgn_1','offset_bgn_1',
                      'seg_end_1','offset_end_1','side_ind_1','terrain_ty','fgrade','c_offset_b','cum_offset','c_offset_1',
                      'cum_offs_1','nlf_cntl_b','nlf_cntl_e','geometry']]
    subset_gdf.sort_values(by=['name', 'fkey'], inplace=True,ascending = ['True',sort_fkey_asc])
    return subset_gdf

i80_eb_sub_gdf = read_data(filename_gdf=path_to_grade_data_file,
                           layer_gdf='SpatialJoinTotal80EB')
i80_eb_sub_gdf.crs

i80_eb_sub_gdf.loc[:,'foffset_diff'] = i80_eb_sub_gdf.groupby(['name','seg_bgn_1']).foffset.diff()
i80_eb_sub_gdf.loc[:,'foffset_diff_max'] = \
    i80_eb_sub_gdf.groupby(['name']).foffset_diff.transform(lambda x: x.abs().max())

i80_eb_sub_gdf_debug = i80_eb_sub_gdf.query('foffset_diff_max>100')

gjson = i80_eb_sub_gdf_debug.to_json()
m = folium.Map([-15.783333, -47.866667],
                  zoom_start=4,
                  tiles='cartodbpositron')

from shapely.geometry import  MultiLineString, mapping, shape

m = folium.Map(tiles='cartodbpositron')
popup_field_list = list(i80_eb_sub_gdf_debug.columns)
popup_field_list.remove('geometry')
feature_grp = folium.FeatureGroup(name='freeval_name')
m.add_child(master_feature_grp)
for i, row in i80_eb_sub_gdf_debug.iterrows():
    sub_feature_grp = \
        plugins.FeatureGroupSubGroup(feature_grp,
                                     f"{row["name"]}")
    m.add_child(sub_feature_grp)
    temp_grp = "test"
    label = '<br>'.join([field + ': ' + str(row[field]) for field in popup_field_list])
    # https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
    line_points = [(tuples[1],tuples[0]) for tuples in list(mapping(row.geometry)['coordinates'][0])]
    if math.isclose(row.foffset_diff, row.foffset_diff_max,rel_tol=1e-2):
        color_line = 'red'; opacity_line = 1; weight_line = 6
    else:
        color_line='blue'; opacity_line=0.5; weight_line = 3
    folium.PolyLine(line_points, color=color_line, weight=weight_line, opacity=opacity_line \
                    , popup=folium.Popup(html=label, parse_html=False, max_width='300')).add_to(sub_feature_grp)
folium.LayerControl(collapsed=True).add_to(m)
m.save(os.path.join(path_to_data,'test.html'))
#https://gis.stackexchange.com/questions/104312/multilinestring-to-separate-individual-lines-using-python-with-gdal-ogr-fiona
# for line in row.geometry:
#     [print(tuples) for tuples in list(line)]


i80_eb_sub_gdf.groupby(['name']).cty_code.apply(lambda x: len(set(x))).max()

i80_wb_sub_gdf = read_data(filename_gdf=path_to_grade_data_file,
                           layer_gdf='SpatialJoinTotal80WB',
                           sort_fkey_asc=False)

with pd.option_context("display.max_rows", 10, "display.max_columns", 20):
    print(i80_eb_sub_gdf)

# clean data


# reshape data

# inventory of freeval segments wtih terrain type 1 and 2---length weighted average


# inventory of freeval segments crossing county lines

# add extra tags to freeval segments when they cross county lines---will be broken manually

# inventory of freeval segments that need to be broken into two (mountaineous terrain)

## add extra tags to freeval segments when grade changes over 2%

# process freeval segments with terrain type 1 and 2

# process freeval segments with terrain type 3
