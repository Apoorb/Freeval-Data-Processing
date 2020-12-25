"""
Summary: read the grade data and do some inital testing.

Created on Tue Aug  4 16:42:33 2020
@author: abibeka

"""
import os
import sys

sys.path.append(
    r"C:\Users\abibeka"
    r"\Github\Freeval-Data-Processing"
    r"\Feeval-PA Scripts\grade_data_processing"
)
import grade_process_mod as gradepr  # noqa E402

# 1.2 Set Global Parameters
path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\raw"
path_interim = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\interim"


if __name__ == "__main__":
    path_to_grade_data_file = os.path.join(path_to_data, "Processing.gdb")
    read_obj = gradepr.ReadGrade(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_interim=path_interim,
        read_saved_shp_csv=False,
        read_saved_csv=False,
    )
    path_to_grade_data_file = os.path.join(path_to_data, "NewGeodatabase.gdb")
    read_obj_new = gradepr.ReadGrade(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_interim=path_interim,
        layers_raw_grade_data=[
            "MissingSections_GRADE",
            "I_376_NB_1_SR422_to_I80_GRADE",
            "I_376_NB_2_I76_to_ConstitutionBlvd_GRADE",
            "I_376_SB_1_I80_to_SR422_GRADE",
            "I_376_SB_2_ConstitutionBlvd_to_I76_GRADE",
            "I_76_NB_1_GRADE",
            "I_76_SB_1_GRADE",
            "PA_581_NB_1_GRADE",
            "PA_581_SB_1_GRADE",
        ],
        read_saved_shp_csv=False,
        read_saved_csv=False,
    )
    # 2 read data and output smaller subsets
    # -----------------------------------------------------------------------------
    # grade_df_dict = read_obj.data_read_switch()

    # Read the new geoprocessing data
    read_obj_new.data_read_switch(
        grade_gdf_asc_save_loc="grade_gdf_asc_sort_i76_376_pa581_plus_missing_data",
        grade_gdf_desc_save_loc="grade_gdf_desc_sort_i76_376_pa581_plus_missing_data",
    )

    grade_df_asc = grade_df_dict["grade_df_asc"]
    grade_df_asc_sort = grade_df_asc.sort_values(
        ["name", "cty_code", "fkey"], ascending=[True, True, True]
    )
    grade_df_asc_sort_unique = grade_df_asc_sort.drop_duplicates(
        subset=["name", "cty_code", "seg_no", "foffset"],
        keep="first",
        ignore_index=True,
    )
    # Quick test
    grade_df_asc_sort_unique.groupby(["st_rt_no", "cty_code"]).name.apply(set)

    # 3. Run tests on I-83 data
    # -----------------------------------------------------------------------------
    # Subset and sort data for route 83 by fseg and foffset
    grade_df_asc_sort_83 = grade_df_asc_sort.query("st_rt_no == 83").sort_values(
        ["fseg", "foffset"]
    )
    # Missing fseg where causing issue when we groupby fseg and sum foffset.
    grade_df_asc_sort_83_unique = grade_df_asc_sort_83.loc[
        ~grade_df_asc_sort_83.fseg.isna()
    ].drop_duplicates(["fseg", "foffset"])
    # Test if the total length of I-83 is same (close) using foffset,
    # c_offset_b,
    # and flength
    grade_df_asc_sort_83_unique = grade_df_asc_sort_83_unique.assign(
        c_offset_b_diff=lambda x: x.groupby(["cty_code", "seg_no"]).c_offset_b.diff(),
        foffset_diff=lambda x: x.groupby(["fseg"]).foffset.diff(),
        name_diff=lambda x: x.name.diff(),
    )
    grade_df_asc_sort_83_unique[["flength", "foffset_diff", "c_offset_b_diff"]].sum()
    grade_df_asc_sort_83_unique[
        ["name_diff", "foffset_diff", "c_offset_b_diff"]
    ].describe()
    # Test if the name increases along with fseg and foffset
    check_name_order = grade_df_asc_sort_83_unique.loc[lambda x: x.name_diff < 0]
    # Test if fkeg increases within a county
    grade_df_asc_sort_83_unique.groupby(["cty_code"]).fkey.diff().describe()
    # Test if seq_no increases irrespective of county along route I-83
    grade_df_asc_sort_83_unique.seq_no.diff().describe()

    # Subset and sort data for route 83 by name, city_code, and fkey;
    # Alexandra's
    # method
    grade_df_asc_sort_83_fkey = (
        grade_df_asc_sort.query("st_rt_no == 1")
        .loc[~grade_df_asc_sort.fkey.isna()]
        .sort_values(["name", "cty_code", "fkey"])
        .drop_duplicates(["fseg", "foffset"])
    )
    # Test if the total length of I-83 is same (close) using foffset,
    # c_offset_b,
    # and flength
    grade_df_asc_sort_83_fkey = grade_df_asc_sort_83_fkey.assign(
        c_offset_b_diff=lambda x: x.groupby(["cty_code", "seg_no"]).c_offset_b.diff(),
        foffset_diff=lambda x: x.groupby(["cty_code", "fseg"]).foffset.diff(),
        name_diff=lambda x: x.groupby(["cty_code"]).name.diff(),
    )
    grade_df_asc_sort_83_fkey[["flength", "foffset_diff", "c_offset_b_diff"]].sum()
    # Test if the name, foffset, and c_offset_b increases along with fseg
    # and foffset
    grade_df_asc_sort_83_fkey[
        ["name_diff", "foffset_diff", "c_offset_b_diff"]
    ].describe()
