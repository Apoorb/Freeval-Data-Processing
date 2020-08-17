"""
Summary: read the grade data and do some inital testing.

Created on Tue Aug  4 16:42:33 2020
@author: abibeka

"""
import os
import sys
sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc"
                r"\Documents\Github\Freeval-Data-Processing"
                r"\Feeval-PA Scripts\grade_data_processing")
import grade_process_mod as gradepr  # noqa E402
# 1.2 Set Global Parameters
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

grade_df_asc = grade_df_dict['grade_df_asc']
grade_df_asc_sort = (grade_df_asc
                     .sort_values(['name', 'cty_code', 'fkey'],
                                  ascending=[True, True, True])
                     )
grade_df_asc_sort_unique = grade_df_asc_sort.drop_duplicates(
    subset=["name", "cty_code", "seg_no", "foffset"],
    keep="first",
    ignore_index=True)
# Quick test
grade_df_asc_sort_unique.groupby(["st_rt_no", "cty_code"]).name.apply(set)

# 3. Run tests on I-83 data
# -----------------------------------------------------------------------------
# Subset and sort data for route 83 by fseg and foffset
grade_df_asc_sort_83 = (grade_df_asc_sort
                        .query("st_rt_no == 83")
                        .sort_values(["fseg", "foffset"])
                        )
# Missing fseg where causing issue when we groupby fseg and sum foffset.
grade_df_asc_sort_83_unique = (
    grade_df_asc_sort_83
    .loc[~ grade_df_asc_sort_83.fseg.isna()]
    .drop_duplicates(["fseg", "foffset"])
    )
# Test if the total length of I-83 is same (close) using foffset, c_offset_b,
# and flength
grade_df_asc_sort_83_unique = (
    grade_df_asc_sort_83_unique
    .assign(c_offset_b_diff=lambda x: x
            .groupby(["cty_code", "seg_no"]).c_offset_b.diff(),
            foffset_diff=lambda x: x
            .groupby(["fseg"]).foffset.diff(),
            name_diff=lambda x: x.name.diff())
    )
grade_df_asc_sort_83_unique[
        ["flength", "foffset_diff", "c_offset_b_diff"]
    ].sum()
grade_df_asc_sort_83_unique[
        ["name_diff", "foffset_diff", "c_offset_b_diff"]
    ].describe()
# Test if the name increases along with fseg and foffset
check_name_order = grade_df_asc_sort_83_unique.loc[lambda x: x.name_diff < 0]
# Test if fkeg increases within a county
grade_df_asc_sort_83_unique.groupby(["cty_code"]).fkey.diff().describe()
# Test if seq_no increases irrespective of county along route I-83
grade_df_asc_sort_83_unique.seq_no.diff().describe()

# Subset and sort data for route 83 by name, city_code, and fkey; Alexandra's
# method
grade_df_asc_sort_83_fkey = (
    grade_df_asc_sort
    .query("st_rt_no == 1")
    .loc[~ grade_df_asc_sort.fkey.isna()]
    .sort_values(["name", "cty_code", "fkey"])
    .drop_duplicates(["fseg", "foffset"])
    )
# Test if the total length of I-83 is same (close) using foffset, c_offset_b,
# and flength
grade_df_asc_sort_83_fkey = (
    grade_df_asc_sort_83_fkey
    .assign(c_offset_b_diff=lambda x: x
            .groupby(["cty_code", "seg_no"]).c_offset_b.diff(),
            foffset_diff=lambda x: x
            .groupby(["cty_code", "fseg"]).foffset.diff(),
            name_diff=lambda x: x
            .groupby(["cty_code"]).name.diff())
    )
grade_df_asc_sort_83_fkey[
        ["flength", "foffset_diff", "c_offset_b_diff"]
    ].sum()
# Test if the name, foffset, and c_offset_b increases along with fseg
# and foffset
grade_df_asc_sort_83_fkey[
        ["name_diff", "foffset_diff", "c_offset_b_diff"]
    ].describe()

# Need to drop duplicates to make the flength sum similar to
# foffset_diff, and c_offset_diff

# inventory of freeval segments wtih terrain type 1 and 2---length weighted
# average


# inventory of freeval segments crossing county lines

# add extra tags to freeval segments when they cross county lines
# ---will be broken manually

# inventory of freeval segments that need to be broken into two
# (mountaineous terrain)

# add extra tags to freeval segments when grade changes over 2%

# process freeval segments with terrain type 1 and 2

# process freeval segments with terrain type 3
