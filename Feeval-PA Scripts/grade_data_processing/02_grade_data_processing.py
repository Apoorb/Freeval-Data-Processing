"""
Purpose: Process the grade data.

Created on Tue Aug  4 16:42:33 2020
@author: abibeka
"""
import pandas as pd
import os
import sys
import numpy as np
sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc"
                r"\Documents\Github\Freeval-Data-Processing"
                r"\Feeval-PA Scripts\grade_data_processing")
import grade_process_mod as gradepr  # noqa E402
# 1.2 Set Global Parameters
read_shape_file = False
path_to_data = (r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc'
                r'\Documents\Freeval-PA\grade_data'
                r'\June_23_2020')
path_to_grade_data_file = os.path.join(path_to_data, 'Processing.gdb')
path_processed_data = os.path.join(path_to_data, "processed_data")
if not os.path.exists(path_processed_data):
    os.mkdir(path_processed_data)

if __name__ == "__main__":
    # 2 read data and output smaller subsets
    # -----------------------------------------------------------------------------
    grade_df_dict = gradepr.data_read_switch(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_processed_data=path_processed_data,
        read_saved_shp_csv=False,
        read_saved_csv=True,
    )
    grade_df_asc = grade_df_dict['grade_df_asc']
    grade_df_desc = grade_df_dict['grade_df_desc']

    test_dict = {}
    sort_order = {"grade_df_asc":[True, True, True],
                  "grade_df_desc":[True, False, False]}
    df_name = "grade_df_asc"
    df = grade_df_asc
    st_rt_no_ = 80

    for st_rt_no_ in set(df.st_rt_no):
        clean_df_dict = gradepr.clean_grade_df(
            grade_df_asc_or_desc=df,
            grade_df_name=df_name,
            route_no=st_rt_no_,
            sort_order_ne_sw=sort_order,
            tolerance_fkey_misclass_per_1=0)

        clean_df_add_stat = (
            gradepr.compute_grade_stats(
                clean_df_dict["correct_sort_df"])
                             )
    #process_grade
    




