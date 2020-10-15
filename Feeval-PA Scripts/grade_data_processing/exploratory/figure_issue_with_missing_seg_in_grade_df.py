import pandas as pd
import glob
import os
import grade_process_mod as gradepr


# 1.2 Set Global Parameters
read_shape_file = False
path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\June_23_2020"
path_to_grade_data_file = os.path.join(path_to_data, "Processing.gdb")
path_processed_data = os.path.join(path_to_data, "processed_data")
if not os.path.exists(path_processed_data):
    os.mkdir(path_processed_data)

path_freeval_grade_dat = os.path.join(path_processed_data, "freeval_grade_data")

if not os.path.exists(path_freeval_grade_dat):
    os.mkdir(path_freeval_grade_dat)
path_freeval_grade_dat_asc = os.path.join(path_freeval_grade_dat, "asc_seg_no_penndot")
if not os.path.exists(path_freeval_grade_dat_asc):
    os.mkdir(path_freeval_grade_dat_asc)

path_freeval_grade_dat_desc = os.path.join(
    path_freeval_grade_dat, "desc_seg_no_penndot"
)
if not os.path.exists(path_freeval_grade_dat_desc):
    os.mkdir(path_freeval_grade_dat_desc)



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
    missing_grade_rows = test.loc[lambda df: df.name.isna()]
    missing_segmentation_rows = test.loc[lambda df: df.rid.isna()] # Only I-83. Happens because of some duplication. Ignore this.


    len(set(freeval_segmentation.rid))
    len(set(grade_df.name))

    freeval_segmentation.rid.astype(str).str.len().min()
    freeval_segmentation.rid.astype(str).str.len().max()
    grade_df.name.astype(str).str.len().min()
    grade_df.name.astype(str).str.len().describe()
    sum(grade_df.name.astype(str).str.len()==12)

    len(set(freeval_segmentation.rid) - set(grade_df.name))

    read_obj = gradepr.ReadGrade(
        path_to_data=path_to_data,
        path_to_grade_data_file=path_to_grade_data_file,
        path_processed_data=path_processed_data,
        read_saved_shp_csv=False,
        read_saved_csv=True,
    )

    grade_df_dict = read_obj.data_read_switch()
    grade_df_asc = grade_df_dict["grade_df_asc"]
    grade_df_desc = grade_df_dict["grade_df_desc"]
    orignal_grade_df = pd.concat([grade_df_asc, grade_df_desc])
    orignal_grade_df_fil = orignal_grade_df.assign(grade_data_found=True,
                                                   name=lambda df: df.name.astype(str).apply(lambda x: x[:8]+"0"+x[8:]+"0")).filter(items=["name","grade_data_found"])
    missing_grade_rows_find_why = missing_grade_rows.merge(orignal_grade_df_fil, left_on="rid", right_on="name", how="left").assign(all_present=lambda df:df.grade_data_found.fillna(False))

    missing_grade_rows_find_why.to_csv(os.path.join(path_processed_data, "missing_grade_data_for_freeval_segmentation.csv"))