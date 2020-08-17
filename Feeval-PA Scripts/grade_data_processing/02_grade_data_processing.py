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

grade_df_desc_sort_test = (grade_df_dict['grade_df_desc']
                           .query("st_rt_no==80")
                           .sort_values(['name', 'fseg', 'foffset'],
                                        ascending=[True, False, False])
                           .drop_duplicates(["name", "fseg", "foffset"])
                           .reset_index(drop=True)
                           .assign(
                               fkey_diff=lambda df: df.groupby(["name",
                                                                "cty_code"])
                               .fkey.diff())
                           )


# Check fkey increases by county within a freeval segment
assert (grade_df_desc_sort_test
        .groupby(["name","cty_code"])
        .fkey.diff().min()) >=0    

for st_rt_no_ in set(grade_df_asc_sort.st_rt_no):
    grade_df_asc_sort_83 = (grade_df_asc
                            .query("st_rt_no == @st_rt_no_")
                            .sort_values(["name", "fseg", "foffset"])
                            .drop_duplicates(["name", "fseg", "foffset"])
                            .reset_index(drop=True)
                            )
    # assert that a freeval name has at most two counties.
    # this allows us to use the previous freeval name and the next
    # freeval name to figure out the sort order
    assert((grade_df_asc_sort_83
           .groupby("name").cty_code.agg(lambda x: len(x.unique())).max()) 
           <= 2)
    # Below swapping will only work if above assertion is True.
    # i.e. a freeval segment has at most 2 counties
    # E.g. why this is needed; route 80: 
    #     name	cty_code	cty_code_count	freeval_seg_jumps	
    #                          f_shift_cty_code	b_shift_cty_code	
    #                                               cty_code_num	
    #                                                   test_correct_order_prev_grp	
    #                                                              test_correct_order_next_grp	
    #                                                                   has_correct_order_prev_freeval_seg	
    #                                                                               has_correct_order_next_freeval_seg	
    #                                                                                       has_correct_order_prev_next_freeval_seg
    # 38	10008030100037	60	1	0	60.0	60.0	0	True	True	True	True	True
    # 39	10008030100038	60	1	0	60.0	10.0	0	True	False	True	False	True
    # Matches the previous one but not the next one. No worries!
    # 40	10008030100039	10	2	0	60.0	16.0	0	False	False	False	True	True
    # 1st county doesn't match the 1st one or the next one.
    # Solution use the information from the 2nd county in this freeval seg.
    # 2nd county in this freeval segment matches county in 10008030100040
    # so *has_correct_order_next_freeval_seg* becomes true.
    # 41	10008030100039	16	2	0	10.0	16.0	1	False	True	False	True	True
    # Matches the next but not the previous. No worries!
    # 42	10008030100040	16	1  	0	16.0	16.0	0	True	True	True	True	True
    # 43	10008030100041	16	1	0	16.0	16.0	0	True	True	True	True	True
    # 44	10008030100042	16	2	0	16.0	60.0	0	True	False	True	False	True
    # Matches the previous so no worries
    # 45	10008030100042	60	2	0	16.0	16.0	1	False	False	True	False	True
    # 2st county doesn't match the 1st one or the next one.
    # Solution use the information from the 1st county in this freeval seg (10008030100042)
    # 1st county in this freeval segment matches county in 10008030100041, so we are good.
    # 46	10008030100043	16	2	0	60.0	60.0	0	False	False	False	False	False
    # Neither matches the previous or the next one. Definitely needs treatment.
    # 47	10008030100043	60	2	0	16.0	16.0	1	False	False	False	False	False
    # Neither matches the previous or the next one. Definitely needs treatment.
    # 48	10008030100044	16	1	0	60.0	16.0	0	False	True	False	True	True
    # 49	10008030100045	16	1	0	16.0	16.0	0	True	True	True	True	True
    # 50	10008030100046	16	1	0	16.0	16.0	0	True	True	True	True	True

    # Group data on freeval segment and county.
    # Assign initial county numbering: cty_code_count: {0, 1}
    #test_correct_order_prev_grp: check if the county is not equal to county in
    # previous group.
    #test_correct_order_next_grp: check if the county is not equal to county in
    # next group.
    # has_correct_order_prev_freeval_seg: mainly for freeval segment with 2 counties;
    # is true if a freeval segments 1st county matches the previous 
    # segment county.
    # has_correct_order_next_freeval_seg: mainly for freeval segment with 2 counties;
    # is true if a freeval segments 2nd county matches the next 
    # segment county.
    # has_correct_order_prev_next_freeval_seg: true 
    # if has_correct_order_prev_freeval_seg or
    # has_correct_order_prev_next_freeval_seg
    # is true meaning there is a continuity in atleast one direction.
    grade_df_asc_sort_83_fix_ord = (
        grade_df_asc_sort_83
        .groupby(["name","cty_code"])
        .cty_code.agg(lambda x: len(x.unique()))
        .rename("cty_code_count").reset_index()
        .assign(cty_code_count=lambda x: 
                x.groupby("name").cty_code_count.transform(sum),
                freeval_seg_jumps=lambda df: df.name.diff().ge(2).cumsum(),
                f_shift_cty_code=lambda df: df.groupby("freeval_seg_jumps")
                .cty_code.shift().fillna(df.cty_code),
                b_shift_cty_code=lambda df: df.groupby("freeval_seg_jumps")
                .cty_code.shift(-1).fillna(df.cty_code),
                cty_code_num=lambda x: x.groupby("name").cty_code.cumcount(),
                test_correct_order_prev_grp=lambda x: 
                    (x.f_shift_cty_code - x.cty_code).eq(0),
                test_correct_order_next_grp=lambda x: 
                    (x.b_shift_cty_code - x.cty_code).eq(0),
                has_correct_order_prev_freeval_seg=lambda x: 
                    x.groupby("name")
                    .test_correct_order_prev_grp.transform(max),
                has_correct_order_next_freeval_seg=lambda x: 
                    x.groupby("name")
                    .test_correct_order_next_grp.transform(max),
                has_correct_order_prev_next_freeval_seg=lambda x: 
                    x.has_correct_order_prev_freeval_seg 
                    | x.has_correct_order_next_freeval_seg
                    )
       ) 
    # Use cty_code_num and has_correct_order to reverse the order
    # nor gate. Would only work when freeval segment has at most
    # 2 counties
    mask = lambda x: ~ x.has_correct_order_prev_next_freeval_seg
    grade_df_asc_sort_83_fix_ord.loc[mask, "cty_code_num"] = (
        grade_df_asc_sort_83_fix_ord
              .loc[mask, ["cty_code_num",
                          "has_correct_order_prev_next_freeval_seg"]]
              .apply(lambda x: int(not (x[0] or x[1])), axis=1)
              )     
    grade_df_asc_sort_83_fix_ord = (
        grade_df_asc_sort_83_fix_ord.filter(items=[
            "name", "cty_code", 
            "cty_code_num", 
            "has_correct_order_prev_next_freeval_seg"]))
    # Reorder the counties within a freeval segment based on above 
    # corrections.
    grade_df_asc_sort_83_cor_cty_code_sort = (
        grade_df_asc_sort_83
        .merge(grade_df_asc_sort_83_fix_ord, 
               on=["name", "cty_code"],
               how="inner")
        .sort_values(["name", "cty_code_num", "fseg", "foffset"])
        .reset_index(drop=True)
        )
    # Check if the above sorting gives the correct order of counties.
    # if cty_code only changes within a freeval segment them we are good.
    grade_df_asc_sort_83_fix_ord_test = (
          grade_df_asc_sort_83_cor_cty_code_sort
          .groupby(["name","cty_code"])
          .cty_code_num.first()
          .rename("cty_code_num").reset_index()
          .sort_values(["name", "cty_code_num"])
          .assign(
              freeval_seg_jumps=lambda df: df.name.diff().ge(2).cumsum(),
                f_shift_cty_code=lambda df: df.groupby("freeval_seg_jumps")
                .cty_code.shift().fillna(df.cty_code),
                b_shift_cty_code=lambda df: df.groupby("freeval_seg_jumps")
                .cty_code.shift(-1).fillna(df.cty_code),
                cty_code_num=lambda x: x.groupby("name").cty_code.cumcount(),
                test_correct_order_prev_grp=lambda x: 
                    (x.f_shift_cty_code - x.cty_code).eq(0),
                test_correct_order_next_grp=lambda x: 
                    (x.b_shift_cty_code - x.cty_code).eq(0),
                has_correct_order_prev_freeval_seg=lambda x: 
                    x.groupby("name")
                    .test_correct_order_prev_grp.transform(max),
                has_correct_order_next_freeval_seg=lambda x: 
                    x.groupby("name")
                    .test_correct_order_next_grp.transform(max),
                has_correct_order_prev_next_freeval_seg=lambda x: 
                    x.has_correct_order_prev_freeval_seg 
                    | x.has_correct_order_next_freeval_seg
                    )
       ) 
    # Check if the above sorting gives the correct order of counties.
    # if cty_code only changes within a freeval segment them we are good.
    assert (grade_df_asc_sort_83_fix_ord_test
            .has_correct_order_prev_next_freeval_seg.all() == True)
    # Check fkey increases by county within a freeval segment
    assert (grade_df_asc_sort_83_cor_cty_code_sort
            .groupby(["name","cty_code"])
            .fkey.diff().min()>=0)   
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
        grade_df_asc_sort_83_cor_cty_code_sort
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


            




