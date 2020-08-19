"""
Module for processing grade data.
"""
import numpy as np
import pandas as pd
import os
import sys


class CleanGrade:
    def __init__(self, grade_df_asc_or_desc_, route, grade_df_name_,
                 sort_order_ne_sw_, tolerance_fkey_misclass_per_):
        self.grade_df_asc_or_desc = grade_df_asc_or_desc_
        self.grade_df_name = grade_df_name_
        self.sort_order_ne_sw = sort_order_ne_sw_
        self.st_rt_no = route
        self.grade_df_asc_or_desc = (
            self.grade_df_asc_or_desc
                .query("st_rt_no == @route")
                .sort_values(
                    ["name", "fseg", "foffset"],
                    ascending=sort_order_ne_sw_[grade_df_name_]
                )
            .drop_duplicates(["name", "fseg", "foffset"])
            .reset_index(drop=True)
        )
        self.tolerance_fkey_misclass_per = tolerance_fkey_misclass_per_
        self.correct_sort_df_test_df = pd.DataFrame()
        self.correct_sort_df = pd.DataFrame()
        self.correct_sort_df_add_stat = pd.DataFrame()

    def clean_grade_df(self):
        """
        Returns
        -------

        """
        print(f"Now cleaning route {self.st_rt_no}...")
        self.test_max_2_county_in_name()
        sort_order_corrector_df = self.fix_sort_order()
        # Reorder the counties within a freeval segment based on above
        # corrections.
        correct_sort_df = (
            self.grade_df_asc_or_desc
                .merge(sort_order_corrector_df,
                       on=["name", "cty_code"],
                       how="inner")
                .sort_values(["name", "cty_code_num", "fseg", "foffset"],
                             ascending=[True] + self.sort_order_ne_sw[
                                 self.grade_df_name])
                .reset_index(drop=True)
        )
        self.correct_sort_df = correct_sort_df
        self.create_df_test_sort()
        self.test_sort_order()
        self.test_fkey_order()
        print("**************************************************************")
        return 1
        # ret_dict = {
        #     "correct_sort_df": correct_sort_df,
        #     "correct_sort_df_test_df": correct_sort_df_test_df, }
        # return ret_dict

    def compute_grade_stats(self):
        """
        Returns
        -------

        """
        # freeval_seg_jumps: finds the set of freeval names are continous
        def func_bin_cut_flength(series, freq_=0.25):
            max_series = max(series)
            period_series = np.ceil(max_series / freq_)
            index_intervals = \
                pd.interval_range(start=0, periods=period_series, freq=freq_)
            return pd.cut(series, index_intervals)
        # freeval_seg_jumps >=2; when there is a gap between freeval segment
        # names
        correct_sort_df_new_stat = (
            self.correct_sort_df
            .assign(name_diff=lambda df1: df1.name.diff(),
                    freeval_seg_jumps=lambda df1: df1.name_diff.ge(2).cumsum(),
                    flength=lambda df1: df1.flength.fillna(21),
                    fgrade_impute=lambda df1:
                    df1.groupby("freeval_seg_jumps").fgrade.bfill().ffill(),
                    cum_flength=lambda df1:
                    df1.groupby("freeval_seg_jumps").flength.cumsum(),
                    cum_flength_mi=lambda df1: df1.cum_flength / 5280,
                    bin_cum_flength_0_25mi=lambda df1:
                    df1.groupby("name").cum_flength_mi
                    .transform(func_bin_cut_flength, freq_=0.25),
                    bin_cum_flength_0_5mi=lambda df1:
                    df1.groupby("name").cum_flength_mi
                    .transform(func_bin_cut_flength, freq_=0.5),
                    max_freeval_seg_grade=lambda df1:
                    df1.groupby("name").fgrade
                    .transform(max),
                    min_freeval_seg_grade=lambda df1:
                    df1.groupby("name").fgrade
                    .transform(min),
                    range_freeval_seg_grade=lambda df1:
                    df1.groupby("name").fgrade
                    .transform(lambda series: series.max() - series.min())
                    )
        )

        def func_weighted_avg(df1):
            return (df1.fgrade * df1.flength).sum() / df1.flength.sum()

        correct_sort_df_new_stat_1 = (
            correct_sort_df_new_stat
            .merge(
                (correct_sort_df_new_stat
                 .groupby(["name", "bin_cum_flength_0_25mi"])
                 .apply(func_weighted_avg)
                 .rename("avg_grade_0_25")
                 .reset_index()),
                on=["name", "bin_cum_flength_0_25mi"],
                how="left"
            )
            .merge(
                (correct_sort_df_new_stat
                 .groupby(["name", "bin_cum_flength_0_5mi"])
                 .apply(func_weighted_avg)
                 .rename("avg_grade_0_5")
                 .reset_index()),
                on=["name", "bin_cum_flength_0_5mi"],
                how="left"
            )
        )
        self.correct_sort_df_add_stat = correct_sort_df_new_stat_1
        # return correct_sort_df_new_stat_1

    # def process_grade_df():
    #     return ""

    def test_max_2_county_in_name(self):
        """
        Returns
        -------

        """
        assert ((self.grade_df_asc_or_desc
                 .groupby("name").cty_code.agg(lambda x: len(x.unique())).max()
                 )
                <= 2), (
            "The current cleaning algorithm will not work if number of"
            "counties in a freeval segment are greater than 2.")
        print("At max 2 counties per freeval segment.")

    def fix_sort_order(self):
        """
        Returns
        -------

        """
        # Below swapping will only work if above assertion is True.
        # i.e. a freeval segment has at most 2 counties
        # E.g. why this is needed; route 80:
        #     name	cty_code	cty_code_count	freeval_seg_jumps
        #                          f_shift_cty_code	b_shift_cty_code
        #                                               cty_code_num
        #                                                   test_correct_order_prev_grp
        #                                                              test_correct_order_next_grp
        #                                                                   has_correct_order_prev_freeval_seg  # noqa E501
        #                                                                               has_correct_order_next_freeval_seg  # noqa E501
        #                                                                                       has_correct_order_prev_next_freeval_seg  # noqa E501
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
        # Solution use the information from the 1st county in this freeval seg (10008030100042)  # noqa E501
        # 1st county in this freeval segment matches county in 10008030100041, so we are good.  # noqa E501
        # 46	10008030100043	16	2	0	60.0	60.0	0	False	False	False	False	False
        # Neither matches the previous or the next one. Definitely needs
        # treatment.
        # 47	10008030100043	60	2	0	16.0	16.0	1	False	False	False	False	False
        # Neither matches the previous or the next one. Definitely needs
        # treatment.
        # 48	10008030100044	16	1	0	60.0	16.0	0	False	True	False	True	True
        # 49	10008030100045	16	1	0	16.0	16.0	0	True	True	True	True	True
        # 50	10008030100046	16	1	0	16.0	16.0	0	True	True	True	True	True

        # Group data on freeval segment and county.
        # Assign initial county numbering: cty_code_count: {0, 1}
        # test_correct_order_prev_grp: check if the county is not equal to
        # county
        # in previous group.
        # test_correct_order_next_grp: check if the county is not equal to
        # county
        # in next group.
        # has_correct_order_prev_freeval_seg: mainly for freeval segment with
        # 2 counties; is true if a freeval segments 1st county matches the
        # previous
        # segment county.
        # has_correct_order_next_freeval_seg: mainly for freeval segment with
        # 2 counties; is true if a freeval segments 2nd county matches the next
        # segment county.
        # has_correct_order_prev_next_freeval_seg: true
        # if has_correct_order_prev_freeval_seg or
        # has_correct_order_prev_next_freeval_seg
        # is true meaning there is a continuity in atleast one direction.
        incorrect_sort_df_fix_ord = (
            self.grade_df_asc_or_desc
            .groupby(["name", "cty_code"])
            .cty_code.agg(lambda x: len(x.unique()))
            .rename("cty_code_count").reset_index()
            .assign(cty_code_count=lambda x: x
                    .groupby("name").cty_code_count.transform(sum),
                    freeval_seg_jumps=lambda df1:
                    df1.name.diff().ge(2).cumsum(),
                    f_shift_cty_code=lambda df1:
                    df1.groupby("freeval_seg_jumps")
                    .cty_code.shift().fillna(df1.cty_code),
                    b_shift_cty_code=lambda df1:
                    df1.groupby("freeval_seg_jumps")
                    .cty_code.shift(-1).fillna(df1.cty_code),
                    cty_code_num=lambda x: x.groupby(
                        "name").cty_code.cumcount(),
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
        incorrect_sort_df_fix_ord.loc[mask, "cty_code_num"] = (
            incorrect_sort_df_fix_ord
            .loc[mask, ["cty_code_num",
                        "has_correct_order_prev_next_freeval_seg"]]
            .apply(lambda x: int(not (x[0] or x[1])), axis=1)
        )
        correct_sort_df = (
            incorrect_sort_df_fix_ord.filter(items=[
                "name", "cty_code",
                "cty_code_num",
                "has_correct_order_prev_next_freeval_seg"]))
        return correct_sort_df

    def create_df_test_sort(self):
        """
        Returns
        -------

        """
        # Check if the above sorting gives the correct order of counties.
        # if cty_code only changes within a freeval segment them we are good.
        self.correct_sort_df_test_df = (
            self.correct_sort_df
            .groupby(["name", "cty_code"])
            .cty_code_num.first()
            .rename("cty_code_num").reset_index()
            .sort_values(["name", "cty_code_num"])
            .assign(
                freeval_seg_jumps=lambda df1: df1.name.diff().ge(2).cumsum(),
                f_shift_cty_code=lambda df1: df1.groupby("freeval_seg_jumps")
                .cty_code.shift().fillna(df1.cty_code),
                b_shift_cty_code=lambda df1: df1.groupby("freeval_seg_jumps")
                .cty_code.shift(-1).fillna(df1.cty_code),
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

    def test_sort_order(self):
        """
        Returns
        -------

        """
        # Check if the above sorting gives the correct order of counties.
        # if cty_code only changes within a freeval segment them we are good.
        assert (self.correct_sort_df_test_df
                .has_correct_order_prev_next_freeval_seg.all() == 1), (
            "Freeval segment going through different counties has issue"
            "with sort order. Look into fix_sort_order function")
        print("Freeval segment and county in correct sort order.")

    def test_fkey_order(self):
        """
        Returns
        -------

        """
        # Check fkey increases by county within a freeval segment
        bad_fkeys = sum(self.correct_sort_df
                        .groupby(["name", "cty_code"])
                        .fkey.diff() < 0)
        all_fkeys = self.correct_sort_df.fkey.count()
        percent_bad_fkey = 100 * bad_fkeys / all_fkeys
        assert percent_bad_fkey <= self.tolerance_fkey_misclass_per
        print(f"fkey in correct sort order. Percent of bad fkeys: "
              f"{percent_bad_fkey}")


if __name__ == "__main__":
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

    grade_df_dict = gradepr.data_read_switch(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_processed_data=path_processed_data,
        read_saved_shp_csv=False,
        read_saved_csv=True,
    )

    grade_df_asc = grade_df_dict['grade_df_asc']
    grade_df_desc = grade_df_dict['grade_df_desc']
    sort_order = {"grade_df_asc": [True, True, True],
                  "grade_df_desc": [True, False, False]}
    df_name = "grade_df_asc"
    df = grade_df_asc
    st_rt_no_ = 80
    tolerance_fkey_misclass_per = 0.1
    asc_grade_obj = gradepr.CleanGrade(
        grade_df_asc_or_desc_=grade_df_asc,
        route=st_rt_no_,
        grade_df_name_="grade_df_asc",
        sort_order_ne_sw_=sort_order,
        tolerance_fkey_misclass_per_=0)
    asc_grade_obj.clean_grade_df()
    asc_grade_obj.compute_grade_stats()
