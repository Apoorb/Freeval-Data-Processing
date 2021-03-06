"""
Purpose: Process the grade data.

Created on Tue Aug  4 16:42:33 2020
@author: abibeka
"""
import pandas as pd
import os
import sys
import numpy as np

sys.path.append(
    r"C:\Users\abibeka"
    r"\Github\Freeval-Data-Processing"
    r"\Feeval-PA Scripts\grade_data_processing"
)
import grade_process_mod as gradepr  # noqa E402

# 1.2 Set Global Parameters
read_shape_file = False
path_common = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents" \
              r"\freeval_pa\grade_data"
path_to_data = os.path.join(path_common, "raw")
path_interim = os.path.join(path_common, "interim")
path_issue = os.path.join(path_common, "issues")
path_processed_data = os.path.join(path_common, "processed")
path_to_grade_data_file = os.path.join(path_to_data, "Processing.gdb")
path_freeval_grade_dat = os.path.join(path_processed_data, "freeval_grade_data")

if not os.path.exists(path_freeval_grade_dat):
    os.mkdir(path_freeval_grade_dat)
path_freeval_grade_dat_asc = os.path.join(path_freeval_grade_dat, "asc_seg_no_penndot")
if not os.path.exists(path_freeval_grade_dat_asc):
    os.mkdir(path_freeval_grade_dat_asc)

path_freeval_grade_dat_desc = os.path.join(
    path_freeval_grade_dat, "desc_seg_no_penndot"
)
if not os.path.exists(path_freeval_grade_dat_desc):
    os.mkdir(path_freeval_grade_dat_desc)

if __name__ == "__main__":
    # 2 read data and output smaller subsets
    # -----------------------------------------------------------------------------
    path_to_grade_data_file = os.path.join(path_to_data, "NewGeodatabase.gdb")
    read_obj_new = gradepr.ReadGrade(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_interim=path_interim,
        layers_raw_grade_data=[],
        read_saved_shp_csv=False,
        read_saved_csv=True,
    )

    grade_df_dict = read_obj_new.data_read_switch(
        grade_gdf_asc_save_loc="grade_gdf_asc_sort_i76_376_pa581_plus_missing_data",
        grade_gdf_desc_save_loc="grade_gdf_desc_sort_i76_376_pa581_plus_missing_data",
    )

    grade_df_asc = grade_df_dict["grade_df_asc"]
    grade_df_desc = grade_df_dict["grade_df_desc"]

    test_dict = {}
    sort_order = {
        "grade_df_asc": [True, True, True],
        "grade_df_desc": [True, False, False],
    }
    df_name = "grade_df_asc"
    df = grade_df_asc
    st_rt_no_ = 80
    asc_grade_obj_dict = {}
    for st_rt_no_ in set(grade_df_asc.st_rt_no):
        asc_grade_obj_dict[st_rt_no_] = gradepr.CleanGrade(
            grade_df_asc_or_desc_=grade_df_asc,
            route=st_rt_no_,
            grade_df_name_="grade_df_asc",
            sort_order_ne_sw_=sort_order,
            tolerance_fkey_misclass_per_=100,
            path_processed_data_=path_processed_data,
            path_issue_=path_issue,
        )
        asc_grade_obj_dict[st_rt_no_].clean_grade_df()
        asc_grade_obj_dict[st_rt_no_].compute_grade_stats()
        asc_grade_obj_dict[st_rt_no_].plot_grade_profile(elevation_start=0)
        dir_1 = asc_grade_obj_dict[st_rt_no_].dir
        path_to_out_file = os.path.join(
            path_freeval_grade_dat_asc, f"new_route_{st_rt_no_}_dir_{dir_1}.csv"
        )
        asc_grade_obj_dict[st_rt_no_].freeval_seg_grade_class.to_csv(
            path_to_out_file, index=False
        )

    desc_grade_obj_dict = {}
    for st_rt_no_ in set(grade_df_desc.st_rt_no):
        desc_grade_obj_dict[st_rt_no_] = gradepr.CleanGrade(
            grade_df_asc_or_desc_=grade_df_desc,
            route=st_rt_no_,
            grade_df_name_="grade_df_desc",
            sort_order_ne_sw_=sort_order,
            tolerance_fkey_misclass_per_=100,
            path_processed_data_=path_processed_data,
            path_issue_=path_issue,
        )
        desc_grade_obj_dict[st_rt_no_].clean_grade_df()
        desc_grade_obj_dict[st_rt_no_].compute_grade_stats()
        desc_grade_obj_dict[st_rt_no_].plot_grade_profile(elevation_start=0)
        dir_1 = desc_grade_obj_dict[st_rt_no_].dir
        path_to_out_file = os.path.join(
            path_freeval_grade_dat_desc, f"new_route_{st_rt_no_}_dir_{dir_1}.csv"
        )
        desc_grade_obj_dict[st_rt_no_].freeval_seg_grade_class.to_csv(
            path_to_out_file, index=False
        )
