import pandas as pd
import glob
import os
import numpy as np

if __name__ == "__main__":
    # Set path for input and output
    path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\raw"
    path_interim = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\interim"
    path_issue = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\issues"
    path_processed_data = (
        r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data" r"\processed"
    )
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
    list_df = []
    for file in asc_files + desc_files:
        list_df.append(pd.read_csv(file))
    grade_df = pd.concat(list_df)

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

    # Output the combined segmentation and grade data.
    freeval_segmentation_grade.to_excel(path_freeval_grade_segmentation, index=False)
