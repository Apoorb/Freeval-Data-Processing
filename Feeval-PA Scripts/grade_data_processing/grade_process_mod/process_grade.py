"""
Module for processing grade data.
"""
import numpy as np
import pandas as pd
import os
import sys
import plotly.express as px
from plotly.offline import plot
from plotly.subplots import make_subplots
import grade_process_mod as gradepr

# FIXME: Add documentation


def func_weighted_avg(df1):
    return (df1.fgrade_impute * df1.flength).sum() / df1.flength.sum()


def calc_seg_leg(seg_len_series):
    return seg_len_series.max() - seg_len_series.min()


class CleanGrade:
    def __init__(
        self,
        grade_df_asc_or_desc_,
        route,
        grade_df_name_,
        sort_order_ne_sw_,
        tolerance_fkey_misclass_per_,
        path_processed_data_,
        path_issue_,
    ):
        self.grade_df_asc_or_desc = grade_df_asc_or_desc_
        self.grade_df_name = grade_df_name_
        self.sort_order_ne_sw = sort_order_ne_sw_
        self.st_rt_no = route
        self.path_processed_data = path_processed_data_
        self.path_issue = path_issue_
        self.grade_df_asc_or_desc = (
            self.grade_df_asc_or_desc.query("st_rt_no == @route")
            .sort_values(
                ["name", "fseg", "foffset"], ascending=sort_order_ne_sw_[grade_df_name_]
            )
            .drop_duplicates(["name", "fseg", "foffset"])
            .reset_index(drop=True)
        )
        self.dir = "_".join(
            np.ravel(self.grade_df_asc_or_desc.dir_ind.unique()).astype(str)
        )
        self.tolerance_fkey_misclass_per = tolerance_fkey_misclass_per_
        self.correct_sort_df_test_df = pd.DataFrame()
        self.correct_sort_df = pd.DataFrame()
        self.correct_sort_df_add_stat = pd.DataFrame()
        self.custom_grade_stat_df = pd.DataFrame()
        self.freeval_seg_grade_class = pd.DataFrame()

    def clean_grade_df(self):
        """
        Get the raw data by a particular route and direction. Test if the maximum number
        of counties per route is 2. Then look at freeval segments before and after to
        determine what the county ordering within a freeval segment should be.
        Returns
        -------
        self.correct_sort_df: pd.DataFrame, data with correct county order.
        self.correct_sort_df_test_df: pd.DataFrame, data used for testing sort order.
        se
        """
        print(f"Now cleaning route {self.st_rt_no} and direction {self.dir}...")
        self.test_max_2_county_in_name()
        sort_order_corrector_df = self.fix_sort_order()
        # Reorder the counties within a freeval segment based on above
        # corrections.
        correct_sort_df = (
            self.grade_df_asc_or_desc.merge(
                sort_order_corrector_df, on=["name", "cty_code"], how="inner"
            )
            .sort_values(
                ["name", "cty_code_num", "fseg", "foffset"],
                ascending=[True] + self.sort_order_ne_sw[self.grade_df_name],
            )
            .reset_index(drop=True)
        )
        self.correct_sort_df = correct_sort_df
        self.create_df_test_sort()
        self.test_sort_order()
        self.test_fkey_order()
        print("**************************************************************")

    def compute_grade_stats(self, grade_smoothening_window=5):
        """
        Get addtional features from the correctly sorted data to help in eventually classify segment types.
        """
        # freeval_seg_jumps >=2; when there is a gap between freeval segment
        # names
        correct_sort_df_new_stat = self.correct_sort_df.assign(
            name_diff=lambda df1: df1.name.diff(),
            freeval_seg_jumps=lambda df1: df1.name_diff.ge(2).cumsum(),
            flength=lambda df1: df1.flength.fillna(21),
            fgrade_impute=lambda df1: df1.groupby("freeval_seg_jumps")
            .fgrade.bfill()
            .ffill()
            .rolling(grade_smoothening_window)
            .mean(),
            cum_flength=lambda df1: df1.groupby("freeval_seg_jumps").flength.cumsum(),
            cum_elevation_tp=lambda df1: df1.flength * df1.fgrade_impute / 100,
            cum_elevation_relative=lambda df1: (df1.cum_elevation_tp.cumsum()),
            cum_flength_mi=lambda df1: 1000 * df1.freeval_seg_jumps
            + (df1.cum_flength / 5280),
            max_freeval_seg_grade=lambda df1: df1.groupby("name")
            .fgrade_impute.transform(max)
            .fillna(df1.fgrade_impute),
            min_freeval_seg_grade=lambda df1: df1.groupby("name")
            .fgrade_impute.transform(min)
            .fillna(df1.fgrade_impute),
            q85_freeval_seg_grade=lambda df1: df1.groupby("name")
            .fgrade_impute.transform(lambda series: series.quantile(0.85))
            .fillna(df1.fgrade_impute),
            range_freeval_seg_grade=lambda df1: df1.max_freeval_seg_grade
            - df1.min_freeval_seg_grade,
            temp_flength_tot=lambda df1: df1.groupby("name").flength.transform(sum),
            temp_freeval_seg_weight_grade=lambda df1: df1.fgrade_impute
            * df1.flength
            / df1.temp_flength_tot,
            avg_grade_freeval_seg=lambda df1: df1.groupby(
                "name"
            ).temp_freeval_seg_weight_grade.transform(sum),
        ).drop(columns=["temp_flength_tot", "temp_freeval_seg_weight_grade"])
        self.correct_sort_df_add_stat = correct_sort_df_new_stat
        self.classify_freeval_seg()

    def classify_freeval_seg(
        self,
        min_grade_vertical_curve_upper_bound=-1,
        max_grade_vertical_curve_lower_bound=2,
        max_grade_grade_change_lower_bound=2,
        range_grade_grade_change_lower_bound=2,
    ):
        """
        Use HCM 6th Ed definition to classify a segment as level, rolling, or having
        specific grade. Use heuristics to determine vertical curve.
        Returns
        -------
        Dataframe with segment classification.
        """
        # Get average grade by segment.
        correct_sort_df_add_stat_agg = (
            self.correct_sort_df_add_stat.filter(
                items=[
                    "freeval_seg_jumps",
                    "name",
                    "q85_freeval_seg_grade",
                    "avg_grade_freeval_seg",
                ]
            )
            .groupby(["name"])
            .agg(
                q85_freeval_seg_grade=("q85_freeval_seg_grade", min),
                avg_grade_freeval_seg=("avg_grade_freeval_seg", min),
            )
            .reset_index()
            .assign(
                grade_over_2=lambda df1: df1.q85_freeval_seg_grade >= 2,
                grade_over_3=lambda df1: df1.q85_freeval_seg_grade >= 3,
            )
        )

        # Get segment length.
        correct_sort_df_add_stat_agg = correct_sort_df_add_stat_agg.merge(
            self.correct_sort_df_add_stat.filter(
                items=[
                    "freeval_seg_jumps",
                    "name",
                    "flength",
                    "cum_flength_mi",
                    "terrain_ty",
                    "min_freeval_seg_grade",
                    "max_freeval_seg_grade",
                    "range_freeval_seg_grade",
                ]
            )
            .assign(
                flength_mi_first=(
                    lambda df1: df1.groupby("name").flength.transform(
                        lambda x: x.iloc[0] / 5280
                    )
                ),
            )
            .groupby(["freeval_seg_jumps", "name"])
            .agg(
                flength_mi_first=("flength_mi_first", min),
                seg_len_temp=("cum_flength_mi", calc_seg_leg),
                min_freeval_seg_grade=("min_freeval_seg_grade", min),
                max_freeval_seg_grade=("max_freeval_seg_grade", min),
                range_freeval_seg_grade=("range_freeval_seg_grade", min),
            )
            .assign(
                seg_len=lambda df1: df1.seg_len_temp + df1.flength_mi_first,
                seg_len_over_025=lambda df1: df1.seg_len >= 0.25,
                seg_len_over_05=lambda df1: df1.seg_len >= 0.5,
                likely_vertical_curve=lambda df1: (
                    df1.max_freeval_seg_grade >= max_grade_vertical_curve_lower_bound
                )
                & (df1.min_freeval_seg_grade <= min_grade_vertical_curve_upper_bound),
                likely_grade_change=lambda df1: (
                    df1.max_freeval_seg_grade >= max_grade_grade_change_lower_bound
                )
                & (df1.range_freeval_seg_grade >= range_grade_grade_change_lower_bound),
            )
            .filter(
                items=[
                    "freeval_seg_jumps",
                    "name",
                    "seg_len",
                    "min_freeval_seg_grade",
                    "max_freeval_seg_grade",
                    "range_freeval_seg_grade",
                    "likely_vertical_curve",
                    "seg_len_over_025",
                    "seg_len_over_05",
                    "likely_grade_change",
                ]
            )
            .reset_index(),
            on="name",
            how="inner",
        )

        # Use 0.25 mile, 0.5 mile, and segment level summary statistics to classify
        # segments.
        correct_sort_df_add_stat_agg = correct_sort_df_add_stat_agg.assign(
            hcm_grade_cat=lambda df1: np.select(
                [
                    df1.likely_vertical_curve,
                    df1.likely_grade_change & (~df1.grade_over_2),
                    df1.likely_grade_change,
                    (df1.seg_len_over_025 & df1.grade_over_3)
                    | (df1.seg_len_over_05 & df1.grade_over_2),
                    df1.grade_over_2,
                    (~df1.grade_over_2),
                ],
                ["VMG", "level", "VMG", "Specific Grade", "Rolling", "Level"],
            )
            # VMG: Variable mountainous grade
        ).reindex(
            columns=[
                "name",
                "hcm_grade_cat",
                "q85_freeval_seg_grade",
                "min_freeval_seg_grade",
                "max_freeval_seg_grade",
                "range_freeval_seg_grade",
                "grade_over_2",
                "seg_len_over_05",
                "grade_over_3",
                "seg_len_over_025",
                "seg_len",
                "likely_vertical_curve",
                "likely_grade_change",
                "avg_grade_freeval_seg",
            ]
        )
        self.freeval_seg_grade_class = correct_sort_df_add_stat_agg

    def plot_grade_profile(self, elevation_start=0):
        """
        Parameters
        ----------
        elevation_start: Manually provide the starting elevation for the route.
        Returns
        -------
        Plots elevation profile and the grade profile.
        """
        temp_plot = (
            self.correct_sort_df_add_stat.merge(
                self.freeval_seg_grade_class.filter(
                    items=[
                        "name",
                        "hcm_grade_cat",
                        "grade_over_2",
                        "seg_len_over_05",
                        "grade_over_3",
                        "seg_len_over_025",
                        "seg_len",
                        "likely_vertical_curve",
                        "likely_grade_change",
                    ]
                ),
                on=["name"],
            )
            .rename(columns={"freeval_seg_jumps": "gap_in_name"})
            .assign(
                cum_elevation_relative=lambda df: df.cum_elevation_relative
                + elevation_start
            )
        )
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            subplot_titles=(
                f"Grade Profile--{self.st_rt_no} and direction {self.dir}",
                f"Relative Cumulative Elevation--{self.st_rt_no} and direction {self.dir}",
            ),
        )

        data_fgrade_impute = px.line(
            temp_plot,
            x="cum_flength_mi",
            y="fgrade_impute",
            color="name",
            line_dash="hcm_grade_cat",
            hover_data=[
                "name",
                "fgrade_impute",
                "cum_elevation_relative",
                "min_freeval_seg_grade",
                "avg_grade_freeval_seg",
                "q85_freeval_seg_grade",
                "max_freeval_seg_grade",
                "range_freeval_seg_grade",
                "seg_len",
                "hcm_grade_cat",
                "grade_over_2",
                "seg_len_over_05",
                "grade_over_3",
                "seg_len_over_025",
                "seg_len",
                "likely_vertical_curve",
                "likely_grade_change",
                "cty_code",
                "dir_ind",
                "fkey",
                "fac_type",
                "fseg",
                "foffset",
            ],
        )["data"]

        data_cum_elvation = px.line(
            temp_plot,
            x="cum_flength_mi",
            y="cum_elevation_relative",
            color="name",
            line_dash="hcm_grade_cat",
            hover_data=[
                "name",
                "fgrade_impute",
                "cum_elevation_relative",
                "min_freeval_seg_grade",
                "avg_grade_freeval_seg",
                "q85_freeval_seg_grade",
                "max_freeval_seg_grade",
                "range_freeval_seg_grade",
                "seg_len",
                "hcm_grade_cat",
                "grade_over_2",
                "seg_len_over_05",
                "grade_over_3",
                "seg_len_over_025",
                "seg_len",
                "likely_vertical_curve",
                "likely_grade_change",
                "cty_code",
                "dir_ind",
                "fkey",
                "fac_type",
                "fseg",
                "foffset",
            ],
        )["data"]

        for dat1, dat2 in zip(data_fgrade_impute, data_cum_elvation):
            fig.add_trace(dat1, row=1, col=1)
            fig.add_trace(dat2, row=2, col=1)
        fig.update_yaxes(title_text="fgrade_impute per 20 ft. (%.)", row=1, col=1)
        fig.update_yaxes(title_text="cum_elevation_relative (ft.)", row=2, col=1)
        fig.update_xaxes(
            title_text="Pseudo Mileposting---Jumps by 1000 mi whenever freeval name "
                       "increases by 2 or more.",
            row=1,
            col=1,
        )
        fig.update_xaxes(
            title_text="Pseudo Mileposting---Jumps by 1000 mi whenever freeval name "
                       "increases by 2 or more.",
            row=2,
            col=1,
        )
        # if y_ == "fgrade_impute":
        #     fig.update_layout(yaxis=dict(range=[-8, 8]))
        # fig.update_yaxes(fixedrange=True)
        if not os.path.exists(os.path.join(self.path_issue, "debug_plots")):
            os.mkdir(os.path.join(self.path_issue, "debug_plots"))
        plot(
            fig,
            filename=os.path.join(
                self.path_issue,
                "debug_plots",
                f"Plots_{self.st_rt_no}_{self.dir}.html",
            ),
            auto_open=False,
        )

    def test_max_2_county_in_name(self):
        """
        Test if there are at max 2 counties in a freeval segment.
        """
        assert (
            self.grade_df_asc_or_desc.groupby("name")
            .cty_code.agg(lambda x: len(x.unique()))
            .max()
        ) <= 2, (
            "The current cleaning algorithm will not work if number of"
            "counties in a freeval segment are greater than 2."
        )
        print("At max 2 counties per freeval segment.")

    def fix_sort_order(self):
        """
        Below swapping will only work if following assertion is True:
            1. a freeval segment has at most 2 counties

        Why this function is needed; E.g. route 80:
            name	cty_code	cty_code_count	freeval_seg_jumps
                                 f_shift_cty_code	b_shift_cty_code
                                                      cty_code_num
                                                          test_correct_order_prev_grp
                                                                     test_correct_order_next_grp
                                                                          has_correct_order_prev_freeval_seg  # noqa E501
                                                                                      has_correct_order_next_freeval_seg  # noqa E501
                                                                                              has_correct_order_prev_next_freeval_seg  # noqa E501
        38	10008030100037	60	1	0	60.0	60.0	0	True	True	True	True	True
        39	10008030100038	60	1	0	60.0	10.0	0	True	False	True	False	True
        Matches the previous one but not the next one. No worries!
        40	10008030100039	10	2	0	60.0	16.0	0	False	False	False	True	True
        1st county doesn't match the 1st one or the next one.
        Solution use the information from the 2nd county in this freeval seg.
        2nd county in this freeval segment matches county in 10008030100040
        so *has_correct_order_next_freeval_seg* becomes true.
        41	10008030100039	16	2	0	10.0	16.0	1	False	True	False	True	True
        Matches the next but not the previous. No worries!
        42	10008030100040	16	1  	0	16.0	16.0	0	True	True	True	True	True
        43	10008030100041	16	1	0	16.0	16.0	0	True	True	True	True	True
        44	10008030100042	16	2	0	16.0	60.0	0	True	False	True	False	True
        Matches the previous so no worries
        45	10008030100042	60	2	0	16.0	16.0	1	False	False	True	False	True
        2st county doesn't match the 1st one or the next one.
        Solution use the information from the 1st county in this freeval seg (10008030100042)  # noqa E501
        1st county in this freeval segment matches county in 10008030100041, so we are good.  # noqa E501
        46	10008030100043	16	2	0	60.0	60.0	0	False	False	False	False	False
        Neither matches the previous or the next one. Definitely needs
        treatment.
        47	10008030100043	60	2	0	16.0	16.0	1	False	False	False	False	False
        Neither matches the previous or the next one. Definitely needs
        treatment.
        48	10008030100044	16	1	0	60.0	16.0	0	False	True	False	True	True
        49	10008030100045	16	1	0	16.0	16.0	0	True	True	True	True	True
        50	10008030100046	16	1	0	16.0	16.0	0	True	True	True	True	True

        Group data on freeval segment and county.
        Assign initial county numbering: cty_code_count: {0, 1}
        test_correct_order_prev_grp: check if the county is not equal to
        county in previous group.
        test_correct_order_next_grp: check if the county is not equal to
        county in next group.
        has_correct_order_prev_freeval_seg: mainly for freeval segment with
        2 counties; is true if a freeval segments 1st county matches the
        previous segment county.
        has_correct_order_next_freeval_seg: mainly for freeval segment with
        2 counties; is true if a freeval segments 2nd county matches the next
        segment county.
        has_correct_order_prev_next_freeval_seg: true
        if has_correct_order_prev_freeval_seg or
        has_correct_order_prev_next_freeval_seg
        is true meaning there is a continuity in atleast one direction.
        """
        incorrect_sort_df_fix_ord = (
            self.grade_df_asc_or_desc.groupby(["name", "cty_code"])
            .cty_code.agg(lambda x: len(x.unique()))
            .rename("cty_code_count")
            .reset_index()
            .assign(
                cty_code_count=lambda x: x.groupby("name").cty_code_count.transform(
                    sum
                ),
                freeval_seg_jumps=lambda df1: df1.name.diff().ge(2).cumsum(),
                f_shift_cty_code=lambda df1: df1.groupby("freeval_seg_jumps")
                .cty_code.shift()
                .fillna(df1.cty_code),
                b_shift_cty_code=lambda df1: df1.groupby("freeval_seg_jumps")
                .cty_code.shift(-1)
                .fillna(df1.cty_code),
                cty_code_num=lambda x: x.groupby("name").cty_code.cumcount(),
                test_correct_order_prev_grp=lambda x: (
                    x.f_shift_cty_code - x.cty_code
                ).eq(0),
                test_correct_order_next_grp=lambda x: (
                    x.b_shift_cty_code - x.cty_code
                ).eq(0),
                has_correct_order_prev_freeval_seg=lambda x: x.groupby(
                    "name"
                ).test_correct_order_prev_grp.transform(max),
                has_correct_order_next_freeval_seg=lambda x: x.groupby(
                    "name"
                ).test_correct_order_next_grp.transform(max),
                has_correct_order_prev_next_freeval_seg=lambda x: x.has_correct_order_prev_freeval_seg
                | x.has_correct_order_next_freeval_seg,
            )
        )
        # Use cty_code_num and has_correct_order to reverse the order
        # nor gate. Would only work when freeval segment has at most
        # 2 counties
        mask = lambda x: ~x.has_correct_order_prev_next_freeval_seg
        incorrect_sort_df_fix_ord.loc[
            mask, "cty_code_num"
        ] = incorrect_sort_df_fix_ord.loc[
            mask, ["cty_code_num", "has_correct_order_prev_next_freeval_seg"]
        ].apply(
            lambda x: int(not (x[0] or x[1])), axis=1
        )
        correct_sort_df = incorrect_sort_df_fix_ord.filter(
            items=[
                "name",
                "cty_code",
                "cty_code_num",
                "has_correct_order_prev_next_freeval_seg",
            ]
        )
        return correct_sort_df

    def create_df_test_sort(self):
        """
        Create dataset to test if the sorting algorithm gives the correct order of
        counties. If cty_code only changes within a freeval segment them we are good.
        """

        self.correct_sort_df_test_df = (
            self.correct_sort_df.groupby(["name", "cty_code"])
            .cty_code_num.first()
            .rename("cty_code_num")
            .reset_index()
            .sort_values(["name", "cty_code_num"])
            .assign(
                freeval_seg_jumps=lambda df1: df1.name.diff().ge(2).cumsum(),
                f_shift_cty_code=lambda df1: df1.groupby("freeval_seg_jumps")
                .cty_code.shift()
                .fillna(df1.cty_code),
                b_shift_cty_code=lambda df1: df1.groupby("freeval_seg_jumps")
                .cty_code.shift(-1)
                .fillna(df1.cty_code),
                cty_code_num=lambda x: x.groupby("name").cty_code.cumcount(),
                test_correct_order_prev_grp=lambda x: (
                    x.f_shift_cty_code - x.cty_code
                ).eq(0),
                test_correct_order_next_grp=lambda x: (
                    x.b_shift_cty_code - x.cty_code
                ).eq(0),
                has_correct_order_prev_freeval_seg=lambda x: x.groupby(
                    "name"
                ).test_correct_order_prev_grp.transform(max),
                has_correct_order_next_freeval_seg=lambda x: x.groupby(
                    "name"
                ).test_correct_order_next_grp.transform(max),
                has_correct_order_prev_next_freeval_seg=lambda x: x.has_correct_order_prev_freeval_seg
                | x.has_correct_order_next_freeval_seg,
            )
        )

    def test_sort_order(self):
        """
        # Check if the sorting algorithm gives the correct order of counties.
        # if cty_code only changes within a freeval segment them we are good.
        """
        assert (
            self.correct_sort_df_test_df.has_correct_order_prev_next_freeval_seg.all()
            == 1
        ), (
            "Freeval segment going through different counties has issue"
            "with sort order. Look into fix_sort_order function"
        )
        print("Freeval segment and county in correct sort order.")

    def test_fkey_order(self):
        """
        Test if fkey is in increasing order (with some error tolerance) for all the
        routes. Odd/ descending segments have some issue with some fkey not in increasing
        order. We are using a tolerance of 1% for these.
        """
        # Check fkey increases by county within a freeval segment
        bad_fkeys = sum(
            self.correct_sort_df.groupby(["name", "cty_code"]).fkey.diff() < 0
        )
        all_fkeys = self.correct_sort_df.fkey.count()
        percent_bad_fkey = 100 * bad_fkeys / all_fkeys
        percent_bad_fkey = np.nan_to_num(percent_bad_fkey)
        print(
            f"fkey in correct sort order. Percent of bad fkeys: " f"{percent_bad_fkey}"
        )
        assert percent_bad_fkey <= self.tolerance_fkey_misclass_per


if __name__ == "__main__":
    sys.path.append(
        r"C:\Users\abibeka"
        r"\Github\Freeval-Data-Processing"
        r"\Feeval-PA Scripts\grade_data_processing"
    )

    # 1.2 Set Global Parameters
    read_shape_file = False
    path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\raw"
    path_interim = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\interim"
    path_to_grade_data_file = os.path.join(path_to_data, "Processing.gdb")
    path_processed_data = (
        r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\processed"
    )
    path_issue = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\issues"

    read_obj = gradepr.ReadGrade(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_interim=path_interim,
        read_saved_shp_csv=False,
        read_saved_csv=True,
    )

    grade_df_dict = read_obj.data_read_switch()
    grade_df_asc = grade_df_dict["grade_df_asc"]
    grade_df_desc = grade_df_dict["grade_df_desc"]
    sort_order = {
        "grade_df_asc": [True, True, True],
        "grade_df_desc": [True, False, False],
    }
    df_name = "grade_df_asc"
    df = grade_df_asc
    st_rt_no_ = 80

    asc_grade_obj = CleanGrade(
        grade_df_asc_or_desc_=df,
        route=st_rt_no_,
        grade_df_name_=df_name,
        sort_order_ne_sw_=sort_order,
        tolerance_fkey_misclass_per_=1,
        path_processed_data_=path_processed_data,
        path_issue_=path_issue,
    )
    asc_grade_obj.clean_grade_df()
    asc_grade_obj.compute_grade_stats()
    asc_grade_obj.plot_grade_profile(elevation_start=928)

    df_name = "grade_df_desc"
    df = grade_df_desc
    st_rt_no_ = 95
    desc_grade_obj = CleanGrade(
        grade_df_asc_or_desc_=df,
        route=st_rt_no_,
        grade_df_name_=df_name,
        sort_order_ne_sw_=sort_order,
        tolerance_fkey_misclass_per_=1,
        path_processed_data_=path_processed_data,
        path_issue_=path_issue,
    )
    desc_grade_obj.clean_grade_df()
    desc_grade_obj.compute_grade_stats()
    desc_grade_obj.plot_grade_profile(elevation_start=0)
