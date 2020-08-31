"""
Purpose: Process the grade data.

Created on Tue Aug  4 16:42:33 2020
@author: abibeka
"""
import pandas as pd
import os
import sys
import folium
from shapely.geometry import MultiLineString, mapping, shape
from folium.plugins import MarkerCluster

sys.path.append(
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc"
    r"\Documents\Github\Freeval-Data-Processing"
    r"\Feeval-PA Scripts\grade_data_processing"
)
import grade_process_mod as gradepr  # noqa E402

# 1.2 Set Global Parameters
read_shape_file = False
path_to_data = (
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc"
    r"\Documents\Freeval-PA\grade_data"
    r"\June_23_2020"
)
path_to_grade_data_file = os.path.join(path_to_data, "Processing.gdb")
path_processed_data = os.path.join(path_to_data, "processed_data")
if not os.path.exists(path_processed_data):
    os.mkdir(path_processed_data)

if __name__ == "__main__":
    # 2 read data and output smaller subsets
    # -----------------------------------------------------------------------------
    read_obj = gradepr.ReadGrade(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_processed_data=path_processed_data,
        read_saved_shp_csv=True,
        read_saved_csv=True,
    )

    grade_df_dict = read_obj.data_read_switch()
    grade_gdf_asc = grade_df_dict["grade_gdf_asc_sort"]
    grade_gdf_desc = grade_df_dict["grade_gdf_desc_sort"]

    sort_order = {
        "grade_gdf_asc": [True, True, True],
        "grade_gdf_desc": [True, False, False],
    }

    df = grade_gdf_desc
    df_name = "grade_gdf_desc"
    grade_gdf_80 = df.loc[lambda x: x.st_rt_no.astype(int) == 80]
    grade_gdf_80 = (
        grade_gdf_80.sort_values(
            ["name", "fseg", "foffset"], ascending=sort_order[df_name]
        )
        .drop_duplicates(["name", "fseg", "foffset"])
        .reset_index(drop=True)
    )
    m = folium.Map(
        tiles="cartodbdark_matter", zoom_start=16, max_zoom=25, control_scale=True
    )

    esri_imagery = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
    esri_attribution = (
        "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, "
        "UPR-EGP, and the GIS User Community"
    )
    folium.TileLayer(
        name="EsriImagery",
        tiles=esri_imagery,
        attr=esri_attribution,
        zoom_start=16,
        max_zoom=25,
        control_scale=True,
    ).add_to(m)
    folium.TileLayer(
        "cartodbpositron", zoom_start=16, max_zoom=20, control_scale=True
    ).add_to(m)
    folium.TileLayer(
        "openstreetmap", zoom_start=16, max_zoom=20, control_scale=True
    ).add_to(m)

    popup_field_list = list(grade_gdf_80.columns)
    popup_field_list.remove("geometry")

    name_feature_grp = {}
    for freeval_name in grade_gdf_80.name.unique():
        name_feature_grp[freeval_name] = MarkerCluster(name=freeval_name, show=False)
        m.add_child(name_feature_grp[freeval_name])

    for i, row in grade_gdf_80.iterrows():
        label = "<br>".join(
            [field + ": " + str(row[field]) for field in popup_field_list]
        )
        # https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
        line_points = [
            (tuples[1], tuples[0])
            for tuples in list(mapping(row.geometry)["coordinates"])
        ]
        line_points_1 = [line_points[0], line_points[-1]]
        color_line = "blue"
        opacity_line = 0.5
        weight_line = 3

        folium.PolyLine(
            line_points_1,
            color=color_line,
            weight=weight_line,
            opacity=opacity_line,
            popup=folium.Popup(html=label, parse_html=False, max_width="300"),
        ).add_to(name_feature_grp[f"{row['name']}"])
    folium.LayerControl(collapsed=True).add_to(m)

    m.save(os.path.join(path_to_data, f"{df_name}_route_80.html"))
