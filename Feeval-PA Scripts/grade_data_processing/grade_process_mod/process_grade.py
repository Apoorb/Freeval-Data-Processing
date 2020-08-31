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
    ):
        self.grade_df_asc_or_desc = grade_df_asc_or_desc_
        self.grade_df_name = grade_df_name_
        self.sort_order_ne_sw = sort_order_ne_sw_
        self.st_rt_no = route
        self.path_processed_data = path_processed_data_
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

    def compute_grade_stats(self):
        """
        Get addtional features from the correctly sorted data to help in eventually classify segment types.
        """
        # freeval_seg_jumps: finds the set of freeval names are continous
        def func_bin_cut_flength(series, freq_=0.25):
            max_series = max(series)
            period_series = np.ceil(max_series / freq_)
            index_intervals = pd.interval_range(
                start=0, periods=period_series, freq=freq_
            )
            return pd.cut(series, index_intervals)

        # freeval_seg_jumps >=2; when there is a gap between freeval segment
        # names
        correct_sort_df_new_stat = self.correct_sort_df.assign(
            name_diff=lambda df1: df1.name.diff(),
            freeval_seg_jumps=lambda df1: df1.name_diff.ge(2).cumsum(),
            flength=lambda df1: df1.flength.fillna(21),
            fgrade_impute=lambda df1: df1.groupby("freeval_seg_jumps")
            .fgrade.bfill()
            .ffill(),
            cum_flength=lambda df1: df1.groupby("freeval_seg_jumps").flength.cumsum(),
            cum_elevation_tp=lambda df1: df1.flength * df1.fgrade_impute / 100,
            cum_elevation_relative=lambda df1: (df1.cum_elevation_tp.cumsum()),
            cum_flength_mi=lambda df1: 1000 * df1.freeval_seg_jumps
            + (df1.cum_flength / 5280),
            bin_cum_flength_0_25mi=lambda df1: df1.groupby(
                "name"
            ).cum_flength_mi.transform(func_bin_cut_flength, freq_=0.25),
            bin_cum_flength_0_5mi=lambda df1: df1.groupby(
                "name"
            ).cum_flength_mi.transform(func_bin_cut_flength, freq_=0.5),
            max_freeval_seg_grade=lambda df1: df1.groupby("name").fgrade.transform(max),
            min_freeval_seg_grade=lambda df1: df1.groupby("name").fgrade.transform(min),
            range_freeval_seg_grade=lambda df1: df1.groupby("name").fgrade.transform(
                lambda series: series.max() - series.min()
            ),
        )
        self.correct_sort_df_add_stat = correct_sort_df_new_stat

    def classify_freeval_seg(self):
        """
        Use HCM 6th Ed definition to classify a segment as level, rolling, or having
        specific grade. Use heuristics to determine vertical curve.
        """
        # Get grade summary stats in 0.25 mile sub-segments.
        # Check if the grade exceeds 3% in all of the 0.25 mile sub-segments
        correct_sort_df_add_stat_025 = (
            self.correct_sort_df_add_stat.groupby(["name", "bin_cum_flength_0_25mi"])
            .apply(func_weighted_avg)
            .rename("avg_grade_0_25")
            .reset_index()
            .assign(
                specfic_grade_025=lambda df: df.groupby("name")[
                    "avg_grade_0_25"
                ].transform(lambda x: x.ge(3).any()),
            )
            .merge(
                self.correct_sort_df_add_stat.groupby(
                    ["name", "bin_cum_flength_0_25mi"]
                )
                .agg(cum_flength_mi_max=("cum_flength_mi", max))
                .reset_index()
                .assign(
                    interval_max=lambda df1: df1.bin_cum_flength_0_25mi.apply(
                        lambda x: x.right
                    ),
                    seg_len_interval=lambda df1: df1.interval_max
                    - df1.cum_flength_mi_max,
                    seg_len_almost_025=lambda df1: df1.seg_len_interval.le(0.1),
                )
                .filter(items=["name", "bin_cum_flength_0_25mi", "seg_len_almost_025"]),
                on=["name", "bin_cum_flength_0_25mi"],
                how="inner",
            )
        )
        # Get grade summary stats in  0.5 mile sub-segments
        # Check if the grade exceeds 2% in all of the 0.5 mile sub-segments
        correct_sort_df_add_stat_05 = (
            self.correct_sort_df_add_stat.groupby(["name", "bin_cum_flength_0_5mi"])
            .apply(func_weighted_avg)
            .rename("avg_grade_0_5")
            .reset_index()
            .assign(
                specfic_grade_05=lambda df: df.groupby("name")[
                    "avg_grade_0_5"
                ].transform(lambda x: x.ge(2).any())
            )
            .merge(
                self.correct_sort_df_add_stat.groupby(["name", "bin_cum_flength_0_5mi"])
                .agg(cum_flength_mi_max=("cum_flength_mi", max))
                .reset_index()
                .assign(
                    interval_max=lambda df1: df1.bin_cum_flength_0_5mi.apply(
                        lambda x: x.right
                    ),
                    seg_len_interval=lambda df1: df1.interval_max
                    - df1.cum_flength_mi_max,
                    seg_len_almost_05=lambda df1: df1.seg_len_interval.le(0.1),
                )
                .filter(items=["name", "bin_cum_flength_0_5mi", "seg_len_almost_05"]),
                on=["name", "bin_cum_flength_0_5mi"],
                how="inner",
            )
        )

        # Check if the grade exceeds 3% in any of the 0.25 mile sub-segments
        correct_sort_df_add_stat_025_agg = (
            correct_sort_df_add_stat_025.loc[lambda df1: df1.seg_len_almost_025]
            .groupby("name")
            .agg(
                specfic_grade_025=("specfic_grade_025", all),
                max_specfic_grade_025=("avg_grade_0_25", max),
            )
            .reset_index()
        )

        # Check if the grade exceeds 2% in any of the 0.5 mile sub-segments
        correct_sort_df_add_stat_05_agg = (
            correct_sort_df_add_stat_05.loc[lambda df1: df1.seg_len_almost_05]
            .groupby("name")
            .agg(
                specfic_grade_05=("specfic_grade_05", all),
                max_specfic_grade_05=("avg_grade_0_5", max),
            )
            .reset_index()
        )

        # Get average grade by segment.
        correct_sort_df_add_stat_agg = (
            self.correct_sort_df_add_stat.filter(
                items=["freeval_seg_jumps", "name", "flength", "fgrade_impute"]
            )
            .groupby(["name"])
            .apply(func_weighted_avg)
            .rename("avg_grade_freeval_seg")
            .reset_index()
            .assign(grade_over_2=lambda df1: df1.avg_grade_freeval_seg >= 2)
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
            )
            .assign(seg_len=lambda df1: df1.seg_len_temp + df1.flength_mi_first,)
            .filter(items=["freeval_seg_jumps", "name", "seg_len", "terrain_ty_list"])
            .reset_index(),
            on="name",
            how="inner",
        )

        self.get_grade_stats_by_custom_len()

        # Merge 0.25 mile, 0.5 mile, and segment level summary statistics.
        correct_sort_df_add_stat_agg_1 = (
            correct_sort_df_add_stat_agg.merge(
                correct_sort_df_add_stat_025_agg, on=["name"], how="left"
            )
            .merge(correct_sort_df_add_stat_05_agg, on=["name"], how="left")
            .merge(self.custom_grade_stat_df, on=["name"], how="left")
            .assign(
                specfic_grade_025=lambda df1: df1.specfic_grade_025.fillna(False),
                specfic_grade_05=lambda df1: df1.specfic_grade_05.fillna(False),
                max_specfic_grade_025=lambda df1: df1.max_specfic_grade_025.fillna(
                    np.nan
                ),
                max_specfic_grade_05=lambda df1: df1.max_specfic_grade_05.fillna(
                    np.nan
                ),
            )
        )

        # Use 0.25 mile, 0.5 mile, and segment level summary statistics to classify segments.
        correct_sort_df_add_stat_agg_1 = correct_sort_df_add_stat_agg_1.assign(
            hcm_grade_cat=lambda df1: np.select(
                [
                    df1.likely_vertical_curve,
                    (
                        (df1.specfic_grade_025 | df1.specfic_grade_05)
                        & (df1.seg_len >= 0.25)
                    ),
                    (
                        (
                            ~(df1.specfic_grade_025 | df1.specfic_grade_05)
                            | (df1.seg_len < 0.25)
                        )
                        & df1.grade_over_2
                    ),
                    (
                        ~(df1.specfic_grade_025 | df1.specfic_grade_05)
                        & (~df1.grade_over_2)
                    ),
                ],
                ["Vertical Curve", "Specific Grade", "Rolling", "Level"],
            )
        ).reindex(
            columns=[
                "name",
                "hcm_grade_cat",
                "avg_grade_freeval_seg",
                "grade_over_2",
                "seg_len",
                "likely_vertical_curve",
                "max_grade_custom_05mi_search",
                "range_grade_custom_05mi_search",
                "specfic_grade_025",
                "max_specfic_grade_025",
                "specfic_grade_05",
                "max_specfic_grade_05",
            ]
        )
        self.freeval_seg_grade_class = correct_sort_df_add_stat_agg_1

    def get_grade_stats_by_custom_len(self):
        """
        Compute statistics for segments by looking within the segments if segment length
        greater than 0.5 miles or search some miles before and after the segment to make a
        0.5 mile custom segment, and look for min, max, and range of grade in this custom
        segment. The max and range of grade in the custom segment helps determine if a
        segment likely has a vertical curve.
        Use heuristics to determine vertical curve:
            max_grade_custom_05mi_search > 2.5%
            range_grade_custom_05mi_search > 6%
        """
        custom_fields_grade = (
            self.correct_sort_df_add_stat.filter(
                items=[
                    "freeval_seg_jumps",
                    "name",
                    "name_diff",
                    "bin_cum_flength_0_25mi",
                    "flength",
                    "bin_cum_flength_0_5mi",
                    "cum_flength_mi",
                    "fgrade_impute",
                    "cum_elevation_relative",
                ]
            )
            .assign(
                flength_mi_first=(
                    lambda df1: df1.groupby("name").flength.transform(
                        lambda x: x.iloc[0] / 5280
                    )
                ),
                cum_flength_mi_reverse=(
                    lambda x: x.groupby("freeval_seg_jumps").cum_flength_mi.transform(
                        lambda x1: x1.max() - x1
                    )
                ),
            )
            .groupby(["freeval_seg_jumps", "name"])
            .agg(
                flength_mi_first=("flength_mi_first", min),
                seg_len_temp=("cum_flength_mi", calc_seg_leg),
                len_before_temp=("cum_flength_mi", min),
                len_after_seg=("cum_flength_mi_reverse", min),
            )
            .assign(
                seg_len=lambda df1: df1.seg_len_temp + df1.flength_mi_first,
                len_before_seg=lambda df1: df1.len_before_temp - df1.flength_mi_first,
                temp_0=0,
                additional_len_need_half=lambda df1: (0.5 - df1.seg_len) / 2,
                additional_len_need_half_1=lambda df1: df1[
                    ["additional_len_need_half", "temp_0"]
                ].max(axis=1),
                check_seg_longer_than_1mi=lambda df1: df1.seg_len >= 0.5,
                len_need_avail_before_seg=lambda df1: df1[
                    ["additional_len_need_half_1", "len_before_seg"]
                ].min(axis=1),
                cum_flength_mi_need_avail_before_seg=lambda df1: df1.len_before_seg
                - df1.len_need_avail_before_seg,
                len_need_avail_after_seg=lambda df1: df1[
                    ["additional_len_need_half_1", "len_after_seg"]
                ].min(axis=1),
                cum_flength_mi_need_avail_after_seg=lambda df1: df1[
                    ["len_before_seg", "len_need_avail_after_seg", "seg_len"]
                ].sum(axis=1),
                check_len_grade_range=lambda df1: df1.cum_flength_mi_need_avail_after_seg
                - df1.cum_flength_mi_need_avail_before_seg,
            )
            .filter(
                items=[
                    "freeval_seg_jumps",
                    "name",
                    "seg_len",
                    "len_before_seg",
                    "len_after_seg",
                    "cum_flength_mi_need_avail_before_seg",
                    "cum_flength_mi_need_avail_after_seg",
                    "check_len_grade_range",
                ]
            )
            .reset_index()
        )

        lookup_grade_mi = self.correct_sort_df_add_stat.filter(
            items=["freeval_seg_jumps", "cum_flength_mi", "fgrade_impute", "flength"]
        )

        def get_min_max_range_avg_grade(df_, lookup_grade_mi_):
            left = df_.cum_flength_mi_need_avail_before_seg
            right = df_.cum_flength_mi_need_avail_after_seg
            lookup_grade_mi_fil = (
                lookup_grade_mi_.loc[
                    lambda x: x.freeval_seg_jumps == df_.freeval_seg_jumps
                ]
                .assign(
                    temp_cut=lambda df1: pd.cut(
                        df1.cum_flength_mi, np.array([left, right])
                    )
                )
                .loc[lambda x: ~x.temp_cut.isna()]
            )
            return (
                lookup_grade_mi_fil.fgrade_impute.min(),
                lookup_grade_mi_fil.fgrade_impute.max(),
                (
                    lookup_grade_mi_fil.fgrade_impute.max()
                    - lookup_grade_mi_fil.fgrade_impute.min()
                ),
                (
                    (
                        lookup_grade_mi_fil.fgrade_impute * lookup_grade_mi_fil.flength
                    ).sum()
                    / lookup_grade_mi_fil.flength.sum()
                ),
            )

        (
            custom_fields_grade["min_grade_custom_05mi_search"],
            custom_fields_grade["max_grade_custom_05mi_search"],
            custom_fields_grade["range_grade_custom_05mi_search"],
            custom_fields_grade["avg_grade_custom_05mi_search"],
        ) = zip(
            *custom_fields_grade.apply(
                get_min_max_range_avg_grade, lookup_grade_mi_=lookup_grade_mi, axis=1
            )
        )

        custom_fields_grade_fil = custom_fields_grade.assign(
            likely_vertical_curve=lambda df1: (df1.max_grade_custom_05mi_search > 2.5)
            & (df1.range_grade_custom_05mi_search > 6)
        ).filter(
            items=[
                "name",
                "max_grade_custom_05mi_search",
                "range_grade_custom_05mi_search",
                "likely_vertical_curve",
            ]
        )
        self.custom_grade_stat_df = custom_fields_grade_fil

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
                self.custom_grade_stat_df, on=["freeval_seg_jumps", "name"]
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
            line_dash="name",
            color="gap_in_name",
            hover_data=[
                "name",
                "cum_flength_mi_need_avail_before_seg",
                "cum_flength_mi_need_avail_after_seg",
                "fgrade_impute",
                "cum_elevation_relative",
                "min_grade",
                "max_grade",
                "range_grade",
                "check_len_grade_range",
                "seg_len",
                "cty_code",
                "dir_ind",
                "fkey",
                "fac_type",
                "fseg",
                "foffset",
                "terrain_ty",
            ],
        )["data"]

        data_cum_elvation = px.line(
            temp_plot,
            x="cum_flength_mi",
            y="cum_elevation_relative",
            line_dash="name",
            color="gap_in_name",
            hover_data=[
                "name",
                "cum_flength_mi_need_avail_before_seg",
                "cum_flength_mi_need_avail_after_seg",
                "fgrade_impute",
                "cum_elevation_relative",
                "min_grade",
                "max_grade",
                "range_grade",
                "check_len_grade_range",
                "seg_len",
                "cty_code",
                "dir_ind",
                "fkey",
                "fac_type",
                "fseg",
                "foffset",
                "terrain_ty",
            ],
        )["data"]

        for dat1, dat2 in zip(data_fgrade_impute, data_cum_elvation):
            fig.add_trace(dat1, row=1, col=1)
            fig.add_trace(dat2, row=2, col=1)
        fig.update_yaxes(title_text="fgrade_impute per 20 ft. (%.)", row=1, col=1)
        fig.update_yaxes(title_text="cum_elevation_relative (ft.)", row=2, col=1)
        fig.update_xaxes(
            title_text="Pseudo Mileposting---Jumps by 1000 mi whenever freeval name increases by 2 or more.",
            row=1,
            col=1,
        )
        fig.update_xaxes(
            title_text="Pseudo Mileposting---Jumps by 1000 mi whenever freeval name increases by 2 or more.",
            row=2,
            col=1,
        )
        # if y_ == "fgrade_impute":
        #     fig.update_layout(yaxis=dict(range=[-8, 8]))
        # fig.update_yaxes(fixedrange=True)
        if not os.path.exists(os.path.join(self.path_processed_data, "debug_plots")):
            os.mkdir(os.path.join(self.path_processed_data, "debug_plots"))
        plot(
            fig,
            filename=os.path.join(
                self.path_processed_data,
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
        assert percent_bad_fkey <= self.tolerance_fkey_misclass_per
        print(
            f"fkey in correct sort order. Percent of bad fkeys: " f"{percent_bad_fkey}"
        )


if __name__ == "__main__":
    sys.path.append(
        r"C:\Users\abibeka"
        r"\Github\Freeval-Data-Processing"
        r"\Feeval-PA Scripts\grade_data_processing"
    )
    import grade_process_mod as gradepr  # noqa E402

    # 1.2 Set Global Parameters
    read_shape_file = False
    path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\June_23_2020"
    path_to_grade_data_file = os.path.join(path_to_data, "Processing.gdb")
    path_processed_data = os.path.join(path_to_data, "processed_data")
    if not os.path.exists(path_processed_data):
        os.mkdir(path_processed_data)

    read_obj = gradepr.ReadGrade(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_processed_data=path_processed_data,
        read_saved_shp_csv=False,
        read_saved_csv=True,
    )

    grade_df_dict = read_obj.data_read_switch()
    # temp = grade_df_dict["grade_gdf_desc_sort"]
    # temp1 = temp.loc[lambda x: x.st_rt_no.astype(int) == 80].head(1000)
    # temp2 = temp.loc[lambda x: x.st_rt_no.astype(int) == 80].tail(1000)
    # temp1 = pd.concat([temp1, temp2])
    # temp1.to_file(os.path.join(path_processed_data, "80_desc_W.geojson"),driver='GeoJSON')

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
    )
    asc_grade_obj.clean_grade_df()
    asc_grade_obj.compute_grade_stats()
    asc_grade_obj.classify_freeval_seg()
    # asc_grade_obj.get_grade_stats_by_custom_len()

    # asc_grade_obj.plot_grade_profile(elevation_start=929)

    df_name = "grade_df_desc"
    df = grade_df_desc
    st_rt_no_ = 80
    desc_grade_obj = CleanGrade(
        grade_df_asc_or_desc_=df,
        route=st_rt_no_,
        grade_df_name_=df_name,
        sort_order_ne_sw_=sort_order,
        tolerance_fkey_misclass_per_=1,
        path_processed_data_=path_processed_data,
    )
    desc_grade_obj.clean_grade_df()
    desc_grade_obj.compute_grade_stats()
    # desc_grade_obj.plot_grade_profile(elevation_start=336)

    shuyi_dir = (
        r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Downloads" r"\I80_PA"
    )
    import numpy as np, os, pandas as pd

    grade_uw = np.genfromtxt(
        os.path.join(shuyi_dir, "grades_I80_PA.txt"), delimiter=","
    )

    miles_uw = np.genfromtxt(os.path.join(shuyi_dir, "miles_I80_PA.txt"), delimiter=",")

    data_uw = pd.DataFrame({"milepost": miles_uw, "grade_percent": grade_uw})
    data_uw_fil = data_uw.query("grade_percent >= -8 & grade_percent < 8")
    data_uw_fil.loc[:, "seg_len"] = data_uw_fil.milepost.diff().fillna(0)
    data_uw_fil.loc[:, "height"] = (data_uw_fil.seg_len * 5280) * (
        data_uw_fil.grade_percent / 100
    )
    data_uw_fil.loc[:, "elevation_in_ft"] = data_uw_fil.height.cumsum() + 929
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        subplot_titles=(
            f"Grade Profile UW--I-80 and direction E",
            f"Elevation UW--I-80 and direction E",
        ),
    )

    data_grade_uw = px.line(data_uw_fil, x="milepost", y="grade_percent",)["data"]

    data_elevation_uw = px.line(data_uw_fil, x="milepost", y="elevation_in_ft",)["data"]

    for dat1, dat2 in zip(data_grade_uw, data_elevation_uw):
        fig.add_trace(dat1, row=1, col=1)
        fig.add_trace(dat2, row=2, col=1)
    fig.update_yaxes(title_text="grade (%.)", row=1, col=1)
    fig.update_yaxes(title_text="cum_elevation_relative (ft.)", row=2, col=1)
    fig.update_xaxes(title_text="Milepost", row=1, col=1)
    fig.update_xaxes(title_text="Milepost", row=2, col=1)
    plot(fig, filename=os.path.join(path_processed_data, "uw_i_80_grade_profile.html"))
