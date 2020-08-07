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
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")
# 1 Import Libraries and Set Global Parameters
# 1.1 Import Libraries
import pandas as pd
import fiona
import os
import folium
import math
import sys
import glob
sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc"
                r"\Documents\Github\Freeval-Data-Processing"
                r"\Feeval-PA Scripts\grade_data_processing")
import grade_process_mod as gradepr

# 1.2 Set Global Parameters
read_shape_file = False
path_to_data = (r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc'
                r'\Documents\Passive Projects\Freeval-PA\grade_data'
                r'\June_23_2020')
path_to_grade_data_file = os.path.join(path_to_data, 'Processing.gdb')
path_processed_data = os.path.join(path_to_data, "processed_data")
if not os.path.exists(path_processed_data):
    os.mkdir(path_processed_data)
# 2 read data and output smaller subsets
# -----------------------------------------------------------------------------
if read_shape_file & (len(glob.glob(os.path.join(path_processed_data, "grade_gdf_asc_sort", "*.shp"))) == 1
    & len(glob.glob(os.path.join(path_processed_data, "grade_gdf_desc_sort", "*.shp"))) == 1):
    grade_gdf_asc_sort = gradepr.read_subset_dat_by_dir(
        path_processed_data = path_processed_data,
        shapefile_nm = "grade_gdf_asc_sort"
    )
    grade_gdf_desc_sort = gradepr.read_subset_dat_by_dir(
        path_processed_data = path_processed_data,
        shapefile_nm = "grade_gdf_desc_sort"
    )
    grade_df_asc = pd.read_csv(os.path.join(path_to_data, "path_processed_data", "grade_asc_data.csv"), index_col=0)
    grade_df_desc = pd.read_csv(os.path.join(path_to_data, "path_processed_data", "grade_desc_data.csv"), index_col=0)
elif read_shape_file:
    fiona.listlayers(path_to_grade_data_file)
    grade_gdf = gradepr.read_data(
        filename_gdf=path_to_grade_data_file,
        layer_gdf='SpatialJoin_GradeDataFINAL'
    )
    grade_gdf_asc_sort, grade_gdf_desc_sort = gradepr.create_subset_dat(
        grade_gdf = grade_gdf
    )
    gradepr.save_subset_dat_by_dir(
        grade_gdf_asc_sort = grade_gdf_asc_sort,
        grade_gdf_desc_sort = grade_gdf_desc_sort,
        path_processed_data = path_processed_data
    )
    grade_df_asc = pd.read_csv(os.path.join(path_processed_data, 
                                            "grade_asc_data.csv"), 
                               index_col = 0)
    grade_df_desc = pd.read_csv(os.path.join(path_processed_data, 
                                             "grade_desc_data.csv"), 
                                index_col = 0)
else:
    grade_df_asc = pd.read_csv(os.path.join(path_processed_data, 
                                            "grade_asc_data.csv"), 
                               index_col = 0)
    grade_df_desc = pd.read_csv(os.path.join(path_processed_data, 
                                             "grade_desc_data.csv"), 
                                index_col = 0)



grade_df_asc_sort = (grade_df_asc
                     .sort_values(
                    ['name','cty_code', 'fkey'], 
                    ascending=[True, True, True])
                    )   


grade_df_asc_sort_unique = grade_df_asc_sort.drop_duplicates(
    subset = ["name", "cty_code", "seg_no", "foffset"],
    keep = "first",
    ignore_index = True)


grade_df_asc_sort_unique.groupby(["st_rt_no","cty_code"]).name.apply(set)



check_freeval_seg_30 = check_freeval_seg[0:10]




grade_df_asc_sort_83 = (grade_df_asc_sort
                        .query("st_rt_no == 1")
                        .sort_values(["fseg", "foffset"])
    )

grade_df_asc_sort_83_unique = (
    grade_df_asc_sort_83
    .loc[~ grade_df_asc_sort_83.fseg.isna()]
    .drop_duplicates(["fseg", "foffset"])
    )

grade_df_asc_sort_83_unique = (
    grade_df_asc_sort_83_unique
    .assign(c_offset_b_diff = lambda x: x
            .groupby(["cty_code", "seg_no"]).c_offset_b.diff(),
            foffset_diff = lambda x: x
            .groupby(["fseg"]).foffset.diff(),
            name_diff = lambda x: x.name.diff())
    )

grade_df_asc_sort_83_unique.groupby(["cty_code"]).fkey.diff().describe()
grade_df_asc_sort_83_unique.seq_no.diff().describe()

grade_df_asc_sort_83_unique[
        ["flength", "foffset_diff", "c_offset_b_diff"]
    ].sum()

grade_df_asc_sort_83_unique[
        ["name_diff", "foffset_diff", "c_offset_b_diff"]
    ].describe()

check = grade_df_asc_sort_83_unique.loc[lambda x: x.name_diff < 0]

grade_df_asc_sort_83_fkey = (
    grade_df_asc_sort
    .query("st_rt_no == 1")
    .loc[~ grade_df_asc_sort.fkey.isna()]
    .sort_values(["name" ,"cty_code", "fkey"])
    .drop_duplicates(["fseg", "foffset"])
    )

grade_df_asc_sort_83_fkey = (
    grade_df_asc_sort_83_fkey
    .assign(c_offset_b_diff = lambda x: x
            .groupby(["cty_code", "seg_no"]).c_offset_b.diff(),
            foffset_diff = lambda x: x
            .groupby(["fseg"]).foffset.diff(),
            name_diff = lambda x: x
            .groupby(["cty_code"]).name.diff())
    )


grade_df_asc_sort_83_fkey[
        ["flength", "foffset_diff", "c_offset_b_diff"]
    ].sum()

grade_df_asc_sort_83_fkey[
        ["name_diff", "foffset_diff", "c_offset_b_diff"]
    ].describe()
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
