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

grade_gdf_asc_sort = grade_df_dict['grade_gdf_asc_sort']

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
    # assert that a freeval name has at most two counties.
    # this allows us to use the previous freeval name and the next
    # freeval name to figure out the sort order
    assert((grade_df_asc_sort_83
           .groupby("name").cty_code.agg(lambda x: len(x.unique())).max()) 
           <= 2)
    # Below swapping will only work if above assertion is True.
    # i.e. a freeval segment has at most 2 counties
    # E.g. why this is needed: 
    # For route 380
    #	name	cty_code	cty_code_count	f_shift_cty_code	cty_code_num	
    # test_incorrect_order	has_correct_order	has_correct_order_1
    #   10	10038010100010	45	1	45.0	0	True	True	True
    #   11	10038010100011	45	1	45.0	0	True	True	True
    #   12	10038010100012	45	2	45.0	0	True	True	True
    #   13	10038010100012	63	2	45.0	1	False	True	True
    #   14	10038010100013	35	2	63.0	0	False	False	False 
    # This should be 63
    #   15	10038010100013	63	2	35.0	1	False	False	False 
    # This should be 35
    #   16	10038010100014	35	1	63.0	0	False	False	True
    #   17	10038010100015	35	1	35.0	0	True	True	True
    #   18	10038010100016	35	1	35.0	0	True	True	True

    # Group data on freeval segment and county.
    # Assign initial county numbering: cty_code_count: {0, 1}
    #test_incorrect_order: check if the county is not equal to county in
    # previous group.
    # has_correct_order: mainly for freeval segment with 2 counties;
    # is true if a freeval segments 1st county matches the previous 
    # segment county.
    # has_correct_order_1: true if has_correct_order and also make all 
    # rows true where freeval segment has only 1 county
    grade_df_asc_sort_83_fix_ord = (
        grade_df_asc_sort_83
        .groupby(["name","cty_code"])
        .cty_code.agg(lambda x: len(x.unique()))
        .rename("cty_code_count").reset_index()
        .assign(cty_code_count=lambda x: 
                x.groupby("name").cty_code_count.transform(sum),
                freeval_seg_jumps=lambda df: df.name.diff().ge(2).cumsum(),
                f_shift_cty_code=lambda df: df.groupby("freeval_seg_jumps")
                .cty_code.shift().bfill(),
                b_shift_cty_code=lambda df: df.groupby("freeval_seg_jumps")
                .cty_code.shift(-1).ffill(),
                cty_code_num=lambda x: x.groupby("name").cty_code.cumcount(),
                test_correct_order=lambda x: 
                    (x.f_shift_cty_code - x.cty_code).eq(0),
                test_correct_order_1=lambda x: 
                    (x.b_shift_cty_code - x.cty_code).eq(0),
                has_correct_order=lambda x: 
                    x.groupby("name").test_correct_order.transform(max),
                has_correct_order_1=lambda x: 
                    x.groupby("name").test_correct_order_1.transform(max),
                has_correct_order_2=lambda x: 
                    x.has_correct_order | x.has_correct_order_1 | x.cty_code_count.eq(1))
       ) 
    # Use cty_code_num and has_correct_order to reverse the order
    # nor gate. Would only work when freeval segment has at most
    # 2 counties
    mask = lambda x: ~ x.has_correct_order_1
    grade_df_asc_sort_83_fix_ord.loc[mask, "cty_code_num"] = (
        grade_df_asc_sort_83_fix_ord
              .loc[mask, ["cty_code_num","has_correct_order_2"]]
              .apply(lambda x: int(not (x[0] or x[1])), axis=1)
              )     
    grade_df_asc_sort_83_fix_ord = (
        grade_df_asc_sort_83_fix_ord.filter(items=["name", "cty_code", 
                                                   "cty_code_num", 
                                                   "has_correct_order"]))
    # Reorder the counties within a freeval segment based on above 
    # corrections.
    grade_df_asc_sort_83 = (
        grade_df_asc_sort_83
        .merge(grade_df_asc_sort_83_fix_ord, 
               on=["name", "cty_code"],
               how="inner")
        .sort_values(["name", "cty_code_num", "fseg", "foffset"])
        .reset_index(drop=True)
        )
    # Check if the above sorting gives the correct order of counties.
    # if cty_code only changes within a freeval segment them we are good.
    #
    # name	cty_code	cty_code_num	test_correct_order	test_correct_order_1
    # 10	10038010100010	45	0	0.0	True
    # 11	10038010100011	45	0	0.0	True
    # 12	10038010100012	45	0	0.0	True
    # 13	10038010100012	63	1	18.0	True 
    # we are fine with county code changing within the freeval name
    # 15	10038010100013	63	0	0.0	True
    # 14	10038010100013	35	1	-28.0	True
    # we are fine with county code changing within the freeval name
    # 16	10038010100014	35	0	0.0	True
    # 17	10038010100015	35	0	0.0	True

    grade_df_asc_sort_83_fix_ord_test = (
          grade_df_asc_sort_83
          .groupby(["name","cty_code"])
          .cty_code_num.first()
          .rename("cty_code_num").reset_index()
          .sort_values(["name", "cty_code_num"])
          .assign(
              freeval_seg_jumps=lambda df: df.name.diff().ge(2).cumsum(),
              test_correct_order=lambda x: x.groupby("freeval_seg_jumps")
              .cty_code.diff().fillna(0),
              test_correct_order_1=lambda x: 
                  x.cty_code_num.eq(0) == x.test_correct_order.eq(0),
              has_correct_order=lambda x: 
                  x.groupby("name").test_correct_order_1.transform(max).ne(0),
              cty_code_count=lambda x: 
                  x.groupby("name").cty_code_num.transform(
                      lambda x: x.count()),
              test_correct_order_2=lambda x: 
                  (x.has_correct_order) | (x.cty_code_count.eq(1)))
         ) 
    assert grade_df_asc_sort_83_fix_ord_test.test_correct_order_2.all() == True
        
    grade_df_asc_sort_83.cty_code.diff().eq(1)
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
    # assert((grade_df_asc_sort_83_clean
    #        .groupby("freeval_seg_jumps").fseg.diff().min()) >= 0)
    
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

