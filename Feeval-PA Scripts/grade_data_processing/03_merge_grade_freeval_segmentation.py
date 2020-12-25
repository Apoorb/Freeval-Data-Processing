import pandas as pd
import glob
import os
import numpy as np

if __name__ == "__main__":
    # Set path for input and output
    path_common = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents" \
                  r"\freeval_pa\grade_data"
    path_to_data = os.path.join(path_common, "raw")
    path_interim = os.path.join(path_common, "interim")
    path_issue = os.path.join(path_common, "issues")
    path_processed_data = os.path.join(path_common, "processed")
    path_freeval_grade_dat = os.path.join(path_processed_data, "freeval_grade_data")
    path_freeval_segmentation_dir = os.path.join(path_to_data, "SegmentationData")
    path_freeval_grade_segmentation = os.path.join(
        path_processed_data, "freeval_seg_grade_comb.xlsx"
    )
    path_freeval_grade_dat_asc = os.path.join(
        path_freeval_grade_dat, "asc_seg_no_penndot"
    )
    path_freeval_grade_dat_desc = os.path.join(
        path_freeval_grade_dat, "desc_seg_no_penndot"
    )
    asc_files = glob.glob(os.path.join(path_freeval_grade_dat_asc, "*.csv"))
    desc_files = glob.glob(os.path.join(path_freeval_grade_dat_desc, "*.csv"))

    # Read the grade data
    import re
    route_no_pat=re.compile(r"(?:new)?_?route_(\d*)_*")
    int(re.search(route_no_pat, "route_8002_dir_E_N_W.csv").group(1))
    list_df = []
    for file in asc_files + desc_files:
        list_df.append(pd.read_csv(file).assign(
            grade_file=os.path.basename(file),
            st_rt_no=lambda df: df.grade_file.str.extract(route_no_pat).astype(int)
            ))
    grade_df = pd.concat(list_df)
    grade_df_no_ramp = grade_df.loc[lambda df: df.st_rt_no < 8000]

    # Read the segmentation data
    seg_df_list = []
    for file in os.listdir(path_freeval_segmentation_dir):
        seg_df_list.append(
            pd.read_csv(os.path.join(path_freeval_segmentation_dir, file))
        )
    freeval_segmentation = pd.concat(seg_df_list)

    # Merge the segmentation data with the grade data.
    freeval_segmentation_grade = freeval_segmentation.assign(
        rid=lambda df: df.rid.astype(str)
    ).merge(
        grade_df_no_ramp.assign(
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
        how="left",
    )

    freeval_segmentation_grade = freeval_segmentation_grade.assign(
        missing_grade_data = lambda df: np.select(
            [
                df.name.isna(),
                ~ df.name.isna()
            ],
            [
                True,
                False
            ]
        )
    )
    freeval_segmentation_grade[["hcm_grade_cat", "q85_freeval_seg_grade"]] = (freeval_segmentation_grade
        .sort_values(["rteType", "rteNum", "rteDirection", "subRouteIdx"])
        .groupby(["rteType", "rteNum", "rteDirection", "subRouteIdx"])
        [["hcm_grade_cat", "q85_freeval_seg_grade"]]
        .fillna(method="bfill").fillna(method="ffill")
     )

    freeval_segmentation_grade = freeval_segmentation_grade.sort_values(
        ["rteType", "rteNum", "rteDirection", "subRouteIdx"])

    missing_grade_rows = freeval_segmentation_grade.loc[lambda df: df.name.isna()]
    print(f"Fill missing {len(missing_grade_rows)} rows in the merged data.")



    freeval_segmentation_grade["flag_dup"]=(
        freeval_segmentation_grade.groupby("rid").rid.transform("count")
    )
    see_duplicates = freeval_segmentation_grade.loc[lambda df: df.flag_dup>1]

    freeval_segmentation_grade_no_duplicates = (
        freeval_segmentation_grade.drop_duplicates("rid", keep="last")
        .drop(columns=["flag_dup"])
    )
    # Output the combined segmentation and grade data.
    (freeval_segmentation_grade_no_duplicates
     .to_excel(path_freeval_grade_segmentation, index=False)
     )