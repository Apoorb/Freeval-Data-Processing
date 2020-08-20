# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:50:29 2019

@author: abibeka
"""
# 0.0 Housekeeping. Clear variable space
# ******************************************************************************************
from IPython import get_ipython  # run magic commands

ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()


import os
import pandas as pd
import matplotlib.pyplot as plt

os.chdir(
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Freeval-PA"
)


# Import the data
# ******************************************************************************************
x1 = pd.ExcelFile("Geometrics_sample.xlsx")
x1.sheet_names
dat = x1.parse("Geometrics")
dat.columns
dat.dtypes
dat.head()
dat.drop(columns="FDIR", inplace=True)
dat.loc[:, "FDIR"] = dat.FSEG.apply(lambda x: "N" if x % 2 == 0 else "S")
# Check that the # of observations for North and South directions
dat.loc[:, "FDIR"].value_counts()
# Sort the values so that the offsets can be used to compute correct lengths
dat.sort_values(["FCOUNTY", "FROUTE", "FDIR", "FSEG", "FOFFSET"], inplace=True)
# Keep only relevant columns
dat1 = dat[["FCOUNTY", "FROUTE", "FDIR", "FSEG", "FOFFSET", "FLENGTH"]].copy()

# Data Cleaning Function
# ******************************************************************************************
def RemoveDuplicatedRows_GetCorLen(data_Comp, dir1="N"):
    """
    data_Comp = Data with both North and South data
    dir1 = "N" or "S" 
    returns:
        dat_Uniq: Cleaned data, 
        Duplicated_dat: Data showing duplicated rows,
        IsTheDataCleaningCorrect : True, if the cleaning is correct, 
        RowsThatNeedDeletion , 
        NumRowsRemoved
    """

    """
    # Get duplicated rows: 
    https://stackoverflow.com/questions/46640945/grouping-by-multiple-columns-to-find-duplicate-rows-pandas
    https://thispointer.com/pandas-find-duplicate-rows-in-a-dataframe-based-on-all-or-selected-columns-using-dataframe-duplicated-in-python/
    """
    SortOrderDict = {"N": [True, True, True, True], "S": [True, True, False, False]}
    data = data_Comp[data_Comp.FDIR == dir1]
    data.sort_values(
        ["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"],
        inplace=True,
        ascending=SortOrderDict[dir1],
    )
    # See What rows are duplicated before deletion
    Duplicated_dat = data[
        data.duplicated(subset=["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"], keep=False)
    ]
    # See Number of rows we are going to delete
    RowsThatNeedDeletion = sum(
        data.duplicated(["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"])
    )

    """
    # Use the max on min values for duplicated rows --- Handle multi-column duplication
    https://stackoverflow.com/questions/32093829/pythonpandas-removing-duplicates-based-on-two-columns-keeping-row-with-max-va
    """
    Length_Orignal_data = data.shape[0]
    # Using the row with the maximum length when offsets are duplicated. Got it insight by looking at the Duplicated_dat_N data
    dat_Uniq = (
        data.groupby(["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"])
        .FLENGTH.max()
        .reset_index()
    )
    Length_UniqueVal_data = dat_Uniq.shape[0]
    # Removed 19 duplicated rows
    NumRowsRemoved = Length_Orignal_data - Length_UniqueVal_data
    IsTheDataCleaningCorrect = RowsThatNeedDeletion == NumRowsRemoved
    dat_Uniq.sort_values(
        ["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"],
        inplace=True,
        ascending=SortOrderDict[dir1],
    )
    dat_Uniq.reset_index(inplace=True, drop=True)
    coeff_dict = {
        "N": -1,
        "S": 1,
    }  # Based on whether offsets are in ascending or descending order
    dat_Uniq["FLen_Cor"] = dat_Uniq.groupby(["FCOUNTY", "FROUTE", "FSEG"])[
        "FOFFSET"
    ].diff(periods=-1) * (coeff_dict[dir1])
    dat_Uniq.loc[dat_Uniq.FLen_Cor.isna(), "FLen_Cor"] = dat_Uniq.loc[
        dat_Uniq.FLen_Cor.isna(), "FLENGTH"
    ]
    dat_Uniq["Error"] = dat_Uniq["FLen_Cor"] - dat_Uniq["FLENGTH"]
    RetList = [
        dat_Uniq,
        Duplicated_dat,
        IsTheDataCleaningCorrect,
        RowsThatNeedDeletion,
        NumRowsRemoved,
    ]
    return RetList


# Process the data
# ******************************************************************************************
dat_N = RemoveDuplicatedRows_GetCorLen(data_Comp=dat1, dir1="N")[0]
dat_S = RemoveDuplicatedRows_GetCorLen(data_Comp=dat1, dir1="S")[0]

# Check for errors
dat_N["Error"].describe()
dat_N["Error"].quantile(0.87)
dat_S["Error"].describe()
dat_S["Error"].quantile(0.87)
# Check for Duplicated Values
sum(dat_N.duplicated(subset=["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"], keep=False))
sum(dat_S.duplicated(subset=["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"], keep=False))
# Get the cumilative sum by county and route
dat_N.loc[:, "RunningSum"] = dat_N.groupby(["FCOUNTY", "FROUTE"])["FLen_Cor"].cumsum()
dat_S.loc[:, "RunningSum"] = dat_S.groupby(["FCOUNTY", "FROUTE"])["FLen_Cor"].cumsum()

merge_on = ["FCOUNTY", "FROUTE", "FSEG", "FOFFSET"]
dat = dat.drop(columns="FLENGTH")
dat_N_out = dat_N.merge(dat, left_on=merge_on, right_on=merge_on, how="left")
dat_S_out = dat_S.merge(dat, left_on=merge_on, right_on=merge_on, how="left")

dat_N_out.to_csv("ProcessedData\\RunningSum_North.csv", index=False)
dat_S_out.to_csv("ProcessedData\\RunningSum_South.csv", index=False)


# Sanity Check 1
# ******************************************************************************************
Dat_Check_N = dat_N.copy()
Dat_Check_S = dat_S.copy()
ls_0 = ["FCOUNTY", "FROUTE", "FSEG"]

Dat_Check_N.loc[:, "RunningSum"] = Dat_Check_N.groupby(ls_0)["FLen_Cor"].cumsum()
Dat_Check_S.loc[:, "RunningSum"] = Dat_Check_S.groupby(ls_0)["FLen_Cor"].cumsum()

Dat_Check_N_a = Dat_Check_N.groupby(ls_0)["RunningSum"].max().reset_index()
Dat_Check_N_b = Dat_Check_N.groupby(ls_0)["FLen_Cor"].sum().reset_index()
Dat_Check_N = Dat_Check_N_a.merge(
    Dat_Check_N_b, left_on=ls_0, right_on=ls_0, how="inner"
)
Dat_Check_N.loc[:, "SanityCheck"] = Dat_Check_N.RunningSum - Dat_Check_N.FLen_Cor

Dat_Check_S_a = Dat_Check_S.groupby(ls_0)["RunningSum"].max().reset_index()
Dat_Check_S_b = Dat_Check_S.groupby(ls_0)["FLen_Cor"].sum().reset_index()
Dat_Check_S = Dat_Check_S_a.merge(
    Dat_Check_S_b, left_on=ls_0, right_on=ls_0, how="inner"
)
Dat_Check_S.loc[:, "SanityCheck"] = Dat_Check_S.RunningSum - Dat_Check_S.FLen_Cor

# Dat_Check_N.to_csv("ProcessedData\\AuxDataCode\\ErrorCheck_1_North.csv",index= False)
# Dat_Check_S.to_csv("ProcessedData\\AuxDataCode\\ErrorCheck_1_South.csv",index= False)


# Sanity Check 2
# ******************************************************************************************
Dat_Check_N_1 = dat_N.copy()
Dat_Check_S_1 = dat_S.copy()

ls = ["FCOUNTY", "FROUTE", "FSEG"]
Dat_Check_N_1.loc[:, "RunningSum"] = Dat_Check_N_1.groupby(
    ["FCOUNTY", "FROUTE", "FSEG"]
)["FLen_Cor"].cumsum()
Dat_Check_N_1.loc[:, "SegLenCheck"] = Dat_Check_N_1.groupby(ls)["FLen_Cor"].cumsum()
Dat_Check_N_1_a = Dat_Check_N_1.groupby(ls).RunningSum.max().reset_index()
Dat_Check_N_1_b = Dat_Check_N_1.groupby(ls).FOFFSET.max().reset_index()
Dat_Check_N_1_1 = Dat_Check_N_1_a.merge(
    Dat_Check_N_1_b, left_on=ls, right_on=ls, how="inner"
)
Dat_Check_N_1_1.loc[:, "SanityCheck"] = (
    Dat_Check_N_1_1.RunningSum - Dat_Check_N_1_1.FOFFSET
)
Dat_Check_N_1_1.loc[:, "LengthLastOffset"] = pd.Series(
    Dat_Check_N_1.groupby(["FCOUNTY", "FROUTE", "FSEG"]).FLen_Cor.last()
).values
Dat_Check_N_1_1.loc[:, "SanityCheck2"] = (
    Dat_Check_N_1_1.loc[:, "LengthLastOffset"] - Dat_Check_N_1_1.loc[:, "SanityCheck"]
)
sum(Dat_Check_N_1_1.loc[:, "SanityCheck2"] == 0)

Dat_Check_S_1.loc[:, "RunningSum"] = Dat_Check_S_1.groupby(
    ["FCOUNTY", "FROUTE", "FSEG"]
)["FLen_Cor"].cumsum()
Dat_Check_S_1.loc[:, "SegLenCheck"] = Dat_Check_S_1.groupby(ls)["FLen_Cor"].cumsum()
Dat_Check_S_1_a = Dat_Check_S_1.groupby(ls).RunningSum.max().reset_index()
Dat_Check_S_1_b = Dat_Check_S_1.groupby(ls).FOFFSET.max().reset_index()
Dat_Check_S_1_1 = Dat_Check_S_1_a.merge(
    Dat_Check_S_1_b, left_on=ls, right_on=ls, how="inner"
)
Dat_Check_S_1_1.loc[:, "SanityCheck"] = (
    Dat_Check_S_1_1.RunningSum - Dat_Check_S_1_1.FOFFSET
)
sum(Dat_Check_S_1_1.loc[:, "SanityCheck"] >= 0)
Dat_Check_N_1_1.to_csv(
    "ProcessedData\\AuxDataCode\\ErrorCheck_2_North.csv", index=False
)
Dat_Check_S_1_1.to_csv(
    "ProcessedData\\AuxDataCode\\ErrorCheck_2_South.csv", index=False
)
# Sanity Check 2
# ******************************************************************************************
# Dat_S_Check.loc[:,'dir'] = 'S'
ls = ["FCOUNTY", "FROUTE", "FSEG"]
Dat_Check_S_1_1.FSEG = Dat_Check_S_1_1.FSEG - 1
Dat_Check_S_1_1.rename(
    columns={"RunningSum": "RunningSum_S", "FOFFSET": "FOFFSET_S"}, inplace=True
)

# Dat_N_Check.loc[:,'dir'] = 'N'
Dat_check_1 = Dat_Check_N_1_1.merge(
    Dat_Check_S_1_1, left_on=ls, right_on=ls, how="inner"
)
Dat_check_1.loc[:, "Diff_NandS"] = Dat_check_1.RunningSum - Dat_check_1.RunningSum_S

# The lengths from 2 directions do not match. Don't know why
