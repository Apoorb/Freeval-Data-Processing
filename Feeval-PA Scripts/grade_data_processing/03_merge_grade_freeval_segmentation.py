import pandas as pd
import glob
import os


if __name__ == "__main__":
    path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\June_23_2020"
    path_processed_data = os.path.join(path_to_data, "processed_data")
    path_freeval_segmentation = os.path.join(path_processed_data,
                                             "full_database.csv")
    path_freeval_grade_dat = os.path.join(path_processed_data, "freeval_grade_data")
    path_freeval_grade_dat_asc = os.path.join(path_freeval_grade_dat,
                                              "asc_seg_no_penndot")
    path_freeval_grade_dat_desc = os.path.join(
        path_freeval_grade_dat, "desc_seg_no_penndot"
    )
    asc_files = glob.glob(os.path.join(path_freeval_grade_dat_asc, "*.csv"))
    desc_files = glob.glob(os.path.join(path_freeval_grade_dat_desc, "*.csv"))

    list_df = []
    for file in asc_files+desc_files:
        list_df.append(pd.read_csv(file))
    grade_df = pd.concat(list_df)

    freeval_segmentation = pd.read_csv(path_freeval_segmentation)
    freeval_segmentation_grade =(
        freeval_segmentation
        .assign(rid=lambda df: df.rid.astype(str))
        .merge(grade_df
            .assign(name=lambda df: df.name.astype(str).apply(lambda x: x[:8]+"0"+x[8:]+"0")),
               left_on="rid",
               right_on="name",
               how="inner")
    )

    test =(
        freeval_segmentation
        .assign(rid=lambda df: df.rid.astype(str))
        .merge(grade_df
            .assign(name=lambda df: df.name.astype(str).apply(lambda x: x[:8]+"0"+x[8:]+"0")),
               left_on="rid",
               right_on="name",
               how="outer")
    )
    test1 = test.loc[lambda df: df.name.isna()]
    test2 = test.loc[lambda df: df.rid.isna()]


len(set(freeval_segmentation.rid))
len(set(grade_df.name))

freeval_segmentation.rid.astype(str).str.len().min()
freeval_segmentation.rid.astype(str).str.len().max()
grade_df.name.astype(str).str.len().min()
grade_df.name.astype(str).str.len().describe()
sum(grade_df.name.astype(str).str.len()==12)

len(set(freeval_segmentation.rid) - set(grade_df.name))

