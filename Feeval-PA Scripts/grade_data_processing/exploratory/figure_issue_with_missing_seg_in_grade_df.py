import pandas as pd
import glob
import os
import grade_process_mod as gradepr
import ast
import geopandas as gpd
from shapely.geometry import Point, LineString
import fiona
import numpy as np


if __name__ == "__main__":
    path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\raw"
    path_interim = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\interim"
    path_issue = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\issues"
    path_to_grade_data_file = os.path.join(path_to_data, "dummy.gdb")
    path_processed_data = (
        r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\processed"
    )
    path_freeval_grade_dat = os.path.join(path_processed_data, "freeval_grade_data")
    path_freeval_segmentation_dir = os.path.join(path_to_data, "SegmentationData")
    path_freeval_grade_dat = os.path.join(path_processed_data, "freeval_grade_data")
    path_freeval_grade_dat_asc = os.path.join(
        path_freeval_grade_dat, "asc_seg_no_penndot"
    )
    path_freeval_grade_dat_desc = os.path.join(
        path_freeval_grade_dat, "desc_seg_no_penndot"
    )
    asc_files = glob.glob(os.path.join(path_freeval_grade_dat_asc, "*.csv"))
    desc_files = glob.glob(os.path.join(path_freeval_grade_dat_desc, "*.csv"))

    list_df = []
    for file in asc_files + desc_files:
        list_df.append(pd.read_csv(file))
    grade_df = pd.concat(list_df)

    seg_df_list = []
    for file in os.listdir(path_freeval_segmentation_dir):
        seg_df_list.append(
            pd.read_csv(os.path.join(path_freeval_segmentation_dir, file))
        )
    freeval_segmentation = pd.concat(seg_df_list)
    freeval_segmentation_grade = freeval_segmentation.assign(
        rid=lambda df: df.rid.astype(str)
    ).merge(
        grade_df.assign(
            name=lambda df: np.select(
                [
                    df.name.astype(str).str.len() == 14,
                    df.name.astype(str).str.len() == 16,
                ],
                [
                    df.name.astype(str).apply(lambda x: x[:8] + "0" + x[8:] + "0"),
                    df.name.astype(str),
                ],
            )
        ),
        left_on="rid",
        right_on="name",
        how="inner",
    )

    test = freeval_segmentation.assign(rid=lambda df: df.rid.astype(str)).merge(
        grade_df.assign(
            name=lambda df: np.select(
                [
                    df.name.astype(str).str.len() == 14,
                    df.name.astype(str).str.len() == 16,
                ],
                [
                    df.name.astype(str).apply(lambda x: x[:8] + "0" + x[8:] + "0"),
                    df.name.astype(str),
                ],
                df.name,
            )
        ),
        left_on="rid",
        right_on="name",
        how="outer",
    )

    missing_grade_rows = test.loc[lambda df: df.name.isna()]

    # Verify that only I-83 has some extra grade data. Happens because of some
    # duplication. Ignore this.
    missing_seg_rows = test.loc[lambda df: df.rid.isna()]
    assert (
        missing_seg_rows.name.astype(str).str.find("10000083").sum() == 0
    ), "All rows are not from I-83"

    len(set(freeval_segmentation.rid))
    len(set(grade_df.name))

    freeval_segmentation.rid.astype(str).str.len().min()
    freeval_segmentation.rid.astype(str).str.len().max()
    grade_df.name.astype(str).str.len().min()
    grade_df.name.astype(str).str.len().describe()
    sum(grade_df.name.astype(str).str.len() == 12)

    len(set(freeval_segmentation.rid) - set(grade_df.name))

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
    orignal_grade_df = pd.concat([grade_df_asc, grade_df_desc])
    orignal_grade_df_fil = orignal_grade_df.assign(
        grade_data_found=True,
        name=lambda df: np.select(
            [df.name.astype(str).str.len() == 14, df.name.astype(str).str.len() == 16,],
            [
                df.name.astype(str).apply(lambda x: x[:8] + "0" + x[8:] + "0"),
                df.name.astype(str),
            ],
        ),
    ).filter(items=["name", "grade_data_found"])
    missing_grade_rows_find_why = missing_grade_rows.merge(
        orignal_grade_df_fil, left_on="rid", right_on="name", how="left"
    ).assign(all_present=lambda df: df.grade_data_found.fillna(False))

    missing_grade_rows_find_why.to_csv(
        os.path.join(path_issue, "missing_grade_data_for_freeval_segmentation_v1.csv")
    )

    missing_grade_rows_find_why.segPolyLine = missing_grade_rows_find_why.segPolyLine.apply(
        lambda x: ast.literal_eval(x.replace("l", ","))
    )

    fiona.supported_drivers["KML"] = "rw"
    missing_grade_rows_find_why.segPolyLine = missing_grade_rows_find_why.segPolyLine.apply(
        lambda x: LineString([Point(i[1], i[0]) for i in x])
    )
    missing_grade_rows_find_why_gdf = gpd.GeoDataFrame(
        missing_grade_rows_find_why, geometry=missing_grade_rows_find_why.segPolyLine
    )
    missing_grade_rows_find_why_gdf.drop(columns="segPolyLine", inplace=True)
    missing_grade_rows_find_why_gdf.crs = "EPSG:4326"
    missing_grade_rows_find_why_gdf.plot()
    missing_grade_rows_find_why_gdf.to_file(
        os.path.join(path_issue, "missing_grade_data_for_freeval_segmentation_v1.kml"),
        driver="KML",
    )
