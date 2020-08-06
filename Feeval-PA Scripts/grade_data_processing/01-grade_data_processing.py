# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 16:42:33 2020

@author: abibeka
"""

# 0 Housekeeping. Clear variable space
# run magic commands
from IPython import get_ipython

ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

# 1 Import Libraries and Set Global Parameters
# 1.1 Import Libraries
import pandas as pd
import fiona
import os
import folium
import math
import sys
sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\Freeval-Data-Processing"
                r"\Feeval-PA Scripts\grade_data_processing")
import grade_process_mod as gradepr

# 1.2 Set Global Parameters
path_to_data = (r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc'
                r'\Documents\Passive Projects\Freeval-PA\grade_data'
                r'\June_23_2020')
path_to_grade_data_file = os.path.join(path_to_data, 'Processing.gdb')
path_processed_data = os.path.join(path_to_data, "processed_data")

# 2 read data
# -----------------------------------------------------------------------------
fiona.listlayers(path_to_grade_data_file)
grade_gdf = gradepr.read_data(
    filename_gdf=path_to_grade_data_file,
    layer_gdf='SpatialJoin_GradeDataFINAL'
)

grade_gdf_asc_sort = (
    grade_gdf
        .loc[lambda x: x.seg_no.astype(int) % 2 == 0]
        .sort_values(by=['name', 'fkey'], ascending=[True, True])
        .reset_index(drop=True)
)

grade_gdf_desc_sort = (
    grade_gdf
        .loc[lambda x: x.seg_no.astype(int) % 2 != 0]
        .sort_values(by=['name', 'fkey'], ascending=[True, False])
        .reset_index(drop=True)
)

if not os.path.exists(path_processed_data):
    os.mkdir(path_processed_data)
if not os.path.exists(os.path.join(path_processed_data,"grade_gdf_asc_sort")):
    os.mkdir(os.path.join(path_processed_data,"grade_gdf_asc_sort"))
if not os.path.exists(os.path.join(path_processed_data, "grade_gdf_desc_sort")):
    os.mkdir(os.path.join(path_processed_data,"grade_gdf_desc_sort"))

grade_gdf_asc_sort_outfile = os.path.join(path_processed_data, "grade_gdf_asc_sort", "grade_gdf_asc_sort.shp")
grade_gdf_desc_sort_outfile = os.path.join(path_processed_data, "grade_gdf_desc_sort", "grade_gdf_desc_sort.shp")
grade_gdf_asc_sort.to_file(driver='ESRI Shapefile', filename=grade_gdf_asc_sort_outfile)
grade_gdf_desc_sort.to_file(driver='ESRI Shapefile', filename=grade_gdf_desc_sort_outfile)


# Check Directions
print("Direction for Ascending data\n",
      grade_gdf_asc_sort.dir_ind.value_counts())

grade_gdf_desc_sort.dir_ind.value_counts()
print("Direction for Descending data\n",
      grade_gdf_desc_sort.dir_ind.value_counts())

grade_df_asc = pd.DataFrame(grade_gdf_asc_sort.drop(columns='geometry'))
grade_df_desc = pd.DataFrame(grade_gdf_desc_sort.drop(columns='geometry'))

grade_df_asc.to_csv(os.path.join(path_to_data, "grade_asc_data.csv"))
grade_df_desc.to_csv(os.path.join(path_to_data, "grade_desc_data.csv"))

grade_gdf_asc_sort.loc[:, 'foffset_diff'] = (
    grade_gdf_asc_sort.groupby(['name', 'seg_no'])
        .c_offset_b.diff()
)

grade_gdf_asc_sort.loc[:, 'foffset_diff_max'] = (
    grade_gdf_asc_sort
        .groupby(['name']).foffset_diff
        .transform(lambda x: x.abs().max())
)

grade_gdf_asc_sort.loc[:, 'c_offset_b_diff'] = (
    grade_gdf_asc_sort.groupby(['name'])
        .c_offset_b.diff()
)

grade_gdf_asc_sort.loc[:, 'c_offset_b_diff_max'] = (
    grade_gdf_asc_sort
        .groupby(['name']).foffset_diff
        .transform(lambda x: x.abs().max())
)

grade_gdf_asc_sort.loc[:, 'count_rows'] = (
    grade_gdf_asc_sort
        .groupby(['name']).c_offset_b_diff
        .transform(lambda x: x.count())
)

temp = grade_gdf_asc_sort.head(10000)

check_freeval_seg = grade_gdf_asc_sort.query('c_offset_b_diff>1000').name.unique()

grade_df_debug = grade_gdf_asc_sort.query("name in @check_freeval_seg")

check_freeval_seg_30 = check_freeval_seg[0:10]
grade_df_debug_1 = grade_gdf_asc_sort.query("name in @check_freeval_seg_30")
grade_df_debug_1 = grade_df_debug_1.query("c_offset_b_diff != 0")

m = folium.Map(tiles='cartodbdark_matter', zoom_start=16, max_zoom=25, control_scale=True)

esri_imagery = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
esri_attribution = \
    "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, " \
    "UPR-EGP, and the GIS User Community"
folium.TileLayer(name="EsriImagery", tiles=esri_imagery, attr=esri_attribution,
                 zoom_start=16, max_zoom=25, control_scale=True).add_to(m)
folium.TileLayer('cartodbpositron', zoom_start=16, max_zoom=20, control_scale=True).add_to(m)
folium.TileLayer('openstreetmap', zoom_start=16, max_zoom=20, control_scale=True).add_to(m)

popup_field_list = list(grade_df_debug_1.columns)
popup_field_list.remove('geometry')

name_feature_grp = {}
for freeval_name in grade_df_debug_1.name.unique():
    name_feature_grp[freeval_name] = \
        folium.FeatureGroup(name=freeval_name)
    m.add_child(name_feature_grp[freeval_name])

for i, row in grade_df_debug_1.iterrows():
    label = '<br>'.join([field + ': ' + str(row[field])
                         for field in popup_field_list])
    # https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
    line_points = [
        (tuples[1], tuples[0])
        for tuples in list(mapping(row.geometry)['coordinates'][0])
    ]
    if math.isclose(row.c_offset_b_diff, row.c_offset_b_diff_max, rel_tol=1e-2):
        color_line = 'red';
        opacity_line = 1;
        weight_line = 6
    else:
        color_line = 'blue';
        opacity_line = 0.5;
        weight_line = 3

    folium.PolyLine(
        line_points,
        color=color_line,
        weight=weight_line,
        opacity=opacity_line,
        popup=folium.Popup(
            html=label,
            parse_html=False,
            max_width='300'
        )
    ).add_to(name_feature_grp[f"{row['name']}"])
folium.LayerControl(collapsed=False).add_to(m)

m.save(os.path.join(path_to_data, 'check_big_offset_jumps.html'))

grade_gdf_asc_sort_clean = grade_gdf_asc_sort.query("c_offset_b_diff < 100")
check_freeval_seg = grade_gdf_asc_sort_clean.name.unique()

grade_df_debug = grade_gdf_asc_sort_clean.query("name in @check_freeval_seg")

check_freeval_seg_100 = check_freeval_seg[0:200]
grade_df_debug_1 = grade_gdf_asc_sort_clean.query("name in @check_freeval_seg_100")
grade_df_debug_1 = grade_df_debug_1.query("c_offset_b_diff != 0")

m = folium.Map(tiles='cartodbdark_matter', zoom_start=16, max_zoom=25, control_scale=True)

esri_imagery = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
esri_attribution = \
    "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, " \
    "UPR-EGP, and the GIS User Community"
folium.TileLayer(name="EsriImagery", tiles=esri_imagery, attr=esri_attribution,
                 zoom_start=16, max_zoom=25, control_scale=True).add_to(m)
folium.TileLayer('cartodbpositron', zoom_start=16, max_zoom=20, control_scale=True).add_to(m)
folium.TileLayer('openstreetmap', zoom_start=16, max_zoom=20, control_scale=True).add_to(m)

popup_field_list = list(grade_df_debug_1.columns)
popup_field_list.remove('geometry')

name_feature_grp = {}
for freeval_name in grade_df_debug_1.name.unique():
    name_feature_grp[freeval_name] = \
        folium.FeatureGroup(name=freeval_name)
    m.add_child(name_feature_grp[freeval_name])

for i, row in grade_df_debug_1.iterrows():
    label = '<br>'.join([field + ': ' + str(row[field])
                         for field in popup_field_list])
    # https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
    line_points = [
        (tuples[1], tuples[0])
        for tuples in list(mapping(row.geometry)['coordinates'][0])
    ]
    if row.c_offset_b_diff > 500:
        color_line = 'red';
        opacity_line = 1;
        weight_line = 6
    else:
        color_line = 'blue';
        opacity_line = 0.5;
        weight_line = 3

    folium.PolyLine(
        line_points,
        color=color_line,
        weight=weight_line,
        opacity=opacity_line,
        popup=folium.Popup(
            html=label,
            parse_html=False,
            max_width='300'
        )
    ).add_to(name_feature_grp[f"{row['name']}"])
folium.LayerControl(collapsed=False).add_to(m)

m.save(os.path.join(path_to_data, 'check_all_offsets.html'))

# https://gis.stackexchange.com/questions/104312/multilinestring-to-separate-individual-lines-using-python-with-gdal-ogr-fiona
# for line in row.geometry:
#     [print(tuples) for tuples in list(line)]


# reshape data

# inventory of freeval segments wtih terrain type 1 and 2---length weighted 
# average


# inventory of freeval segments crossing county lines

# add extra tags to freeval segments when they cross county lines
# ---will be broken manually

# inventory of freeval segments that need to be broken into two 
# (mountaineous terrain)

## add extra tags to freeval segments when grade changes over 2%

# process freeval segments with terrain type 1 and 2

# process freeval segments with terrain type 3
