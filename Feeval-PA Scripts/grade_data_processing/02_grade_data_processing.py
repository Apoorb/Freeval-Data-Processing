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
# 2 read data and output smaller subsets
# -----------------------------------------------------------------------------
grade_df_dict = gradepr.data_read_switch(
    path_to_data=path_to_data,
    path_to_grade_data_file=path_to_grade_data_file,
    path_processed_data=path_processed_data,
    read_saved_shp_csv=True,
    read_saved_csv=False,
)

grade_gdf_asc_sort = grade_df_dict2['grade_gdf_asc_sort']

grade_df_asc = grade_df_dict['grade_df_asc']
grade_df_asc_sort = (grade_df_asc
                     .sort_values(['name', 'cty_code', 'fkey'],
                                  ascending=[True, True, True])
                     )

all_freeval_name_83_grade_dat = set(
    grade_df_asc_sort
    .query("st_rt_no == 83")
    .name)

grade_df_asc_sort_83_fkey = (
    grade_df_asc_sort
    .query("st_rt_no == 83")
    .sort_values(["name", "cty_code", "fkey"])
    .drop_duplicates(["name", "fseg", "foffset"])
    )


for st_rt_no_ in set(grade_df_asc_sort.st_rt_no):
    grade_df_asc_sort_83 = (grade_df_asc_sort
                            .query("st_rt_no == @st_rt_no_")
                            #.query("name <= 100000830115")
                            .sort_values(["name", "fseg", "foffset"])
                            .drop_duplicates(["name", "fseg", "foffset"])
                            )
    
    
    assert(grade_df_asc_sort_83.groupby(["name","fseg"]).foffset.diff().min() >= 0)

    # The freeval names repeat after 100000830115 for some reason
    # freeval_seg_jumps: finds the set of freeval names are continous
    def func_bin_cut_flength(series, freq_=0.25):
        min_series = 0
        max_series = max(series)
        period_series = np.ceil(max_series/freq_)
        index_intervals = \
            pd.interval_range(start=0, periods=period_series, freq=freq_)
        return pd.cut(series, index_intervals)
    
    # freeval_seg_jumps >=2; when there is a gap between freeval segment names
    grade_df_asc_sort_83_clean = (
        grade_df_asc_sort_83
        .assign(name_diff=lambda df: df.name.diff(),
                freeval_seg_jumps=lambda df: df.name_diff.ge(2).cumsum(),
                flength=lambda df: df.flength.fillna(21),
                fgrade_impute=lambda df:
                    df.groupby("freeval_seg_jumps").fgrade.bfill().ffill(),
                cum_flength=lambda df:
                    df.groupby("freeval_seg_jumps").flength.cumsum(),
                cum_flength_mi=lambda df: df.cum_flength / 5280,
                bin_cum_flength_0_25mi=lambda df:
                    df.groupby("name").cum_flength_mi
                    .transform(func_bin_cut_flength, freq_=0.25),
                bin_cum_flength_0_5mi=lambda df:
                    df.groupby("name").cum_flength_mi
                    .transform(func_bin_cut_flength, freq_=0.5),
                max_freeval_seg_grade=lambda df:
                    df.groupby("name").fgrade
                    .transform(max),
                min_freeval_seg_grade=lambda df:
                    df.groupby("name").fgrade
                    .transform(min),
                range_freeval_seg_grade = lambda df:
                    df.groupby("name").fgrade
                    .transform(lambda series: series.max() - series.min())
                    )  
            )
        
    # Test to make sure that even though we sorted by "name", "fseg", and 
    # "foffset"---"fseg" increases across county lines and freeval segments.
    assert((grade_df_asc_sort_83_fkey_clean
           .groupby("freeval_seg_jumps").fseg.diff().min()) >= 0)
    
    def func_weighted_avg(df):
        return (df.fgrade * df.flength).sum() / df.flength.sum()
    grade_df_asc_sort_83_clean_agg_grade = (
        grade_df_asc_sort_83_clean
        .merge(
           (grade_df_asc_sort_83_clean
           .groupby(["name", "bin_cum_flength_0_25mi"])
           .apply(func_weighted_avg)
           .rename("avg_grade_0_25")
           .reset_index()),
           on=["name", "bin_cum_flength_0_25mi"],
           how="left"
            )
        .merge(
        (grade_df_asc_sort_83_clean
        .groupby(["name", "bin_cum_flength_0_5mi"])
        .apply(func_weighted_avg)
        .rename("avg_grade_0_5")
        .reset_index()),
        on=["name", "bin_cum_flength_0_5mi"],
        how="left"
        )
    )


            



grade_df_asc_sort_83_fkey_clean.loc[lambda df: df.fgrade.isna()]

