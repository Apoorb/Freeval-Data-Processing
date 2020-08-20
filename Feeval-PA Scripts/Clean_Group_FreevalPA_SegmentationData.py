# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 08:55:16 2020

@author: abibeka
"""
from collections import defaultdict

import os
import pandas as pd
import numpy as np
import subprocess
import glob
from pathlib import Path
import sys

sys.path.append(
    os.path.abspath(
        r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github"
        r"\Freeval-Data-Processing\Feeval-PA Scripts"
    )
)
from CommonFunctions_FreevalPA_Cleaning import GetVariableSummary
from CommonFunctions_FreevalPA_Cleaning import GetProbData
from CommonFunctions_FreevalPA_Cleaning import CleanAADT_1stLevel
from CommonFunctions_FreevalPA_Cleaning import CleanAADT_2ndLevel
from CommonFunctions_FreevalPA_Cleaning import CleanCityCode_1stLevel
from CommonFunctions_FreevalPA_Cleaning import CleanCityCode_2ndLevel
from CommonFunctions_FreevalPA_Cleaning import PlotlyDebugFigs
from CommonFunctions_FreevalPA_Cleaning import PlotlyDebugFigs_2
from CommonFunctions_FreevalPA_Cleaning import MergeMultipleData
from CommonFunctions_FreevalPA_Cleaning import Clean_Tot_Width_1stLevel
from CommonFunctions_FreevalPA_Cleaning import CleanTotWidth_2ndLevel
from CommonFunctions_FreevalPA_Cleaning import CleanDivsrType_2ndLevel
from CommonFunctions_FreevalPA_Cleaning import CleanDivsrType_1stLevel

# Change the default stacking
from plotly.offline import plot
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1 Get all the files. Store file and sheetname in a dataframe. Make sure 1 sheet per file
# ****************************************************************************************************************************
os.chdir(
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Passive Projects\Freeval-PA\Intersected_Tables_XLS"
)
ListFiles = glob.glob("*.xls")
# Understand the file name and the sheet name
FileData = pd.DataFrame()
for file in ListFiles:
    x1 = pd.ExcelFile(file)
    assert len(x1.sheet_names) == 1, "More than 1 sheet"
    temp = pd.DataFrame({"FileName": file, "SheetName": x1.sheet_names})
    FileData = pd.concat([FileData, temp])
FileData.reset_index(drop=True, inplace=True)

# Sample
file = ListFiles[0]
x1 = pd.ExcelFile(file)
x1.parse().columns
# ****************************************************************************************************************************


# 2 Get summary of duplicates in the data
# ****************************************************************************************************************************
# NumDuplicates = np.empty(0)
ProbDat = pd.DataFrame()
NumRowsDat = pd.DataFrame()
Features = [
    "CUR_AADT",
    "ST_RT_NO",
    "CTY_CODE",
    "DISTRICT_N",
    "JURIS",
    "DIR_IND",
    "FAC_TYPE",
    "TOTAL_WIDT",
    "LANE_CNT",
    "DIVSR_TYPE",
    "DIVSR_WIDT",
    "TRAF_RT_NO",
    "TRAF_RT__1",
    "URBAN_RURA",
    "TRK_PCT",
    "K_FACTOR",
    "D_FACTOR",
    "T_FACTOR",
]
Features_RetDict = GetVariableSummary("I_80_EB_1.xls", Features)


for _, row in FileData.iterrows():
    Features_RetDict = GetVariableSummary(row["FileName"], Features)
    # Get the # of unique Freeval segment Names per file
    TempDict_0 = {
        "Name": [row["FileName"]],
        "Rows": [Features_RetDict["CUR_AADT"]["Ret1"].shape[0]],
    }
    Temp_0 = pd.DataFrame.from_dict(TempDict_0)
    NumRowsDat = pd.concat([NumRowsDat, Temp_0])

    # For each feature get the # of duplicates
    for feature in Features:
        DatDict = Features_RetDict[feature]
        temp = np.squeeze(
            DatDict["Ret1"].loc[:, feature].apply(lambda x: len(x)).values
        )
        if np.max(temp) >= 2:
            # Plot here so that you don't have too many plots
            Path("ProcessedData/Fig/{}/".format(feature)).mkdir(
                parents=True, exist_ok=True
            )
            # PlotlyDebugFigs(DatDict['Ret2'].reset_index(),np.max(temp),feature, row['SheetName'],"ProcessedData/Fig/{}/".format(feature))

        ProbRows = GetProbData(DatDict["Ret1"], feature, row["FileName"])
        ProbDat = pd.concat([ProbDat, ProbRows])
# ****************************************************************************************************************************


# 3 Look at the problem files and type of issues
# ****************************************************************************************************************************
# NumDuplicates = np.empty(0)
ProbDat.columns
ProbDatSum = ProbDat.groupby(["FileName", "FeatureNm", "NumDuplicates"]).agg(
    {"NumDuplicates": {"Tot": "size"}}
)
ProbDatSum.columns = ProbDatSum.columns.droplevel(0)
ProbDatSum = ProbDatSum.reset_index()
ProbDict = {}
for feature in Features:
    ProbDict[feature] = ProbDatSum[ProbDatSum.FeatureNm == feature]

# Rough
ProbDict["DIR_IND"].Tot.sum()
ProbDict["FAC_TYPE"].Tot.sum()
ProbDict["TOTAL_WIDT"].Tot.sum()
ProbDict["LANE_CNT"].Tot.sum()

Prob_Dir = ProbDat[~ProbDat.DIR_IND.isna()]
Prob_FAC_TYPE = ProbDat[~ProbDat.FAC_TYPE.isna()]
Prob_TOTAL_WIDT = ProbDat[~ProbDat.TOTAL_WIDT.isna()]
Prob_LANE_CNT = ProbDat[~ProbDat.LANE_CNT.isna()]

Prob_DIVSR_TYPE = ProbDat[~ProbDat.DIVSR_TYPE.isna()]
Prob_DIVSR_WIDT = ProbDat[~ProbDat.DIVSR_WIDT.isna()]
# Prob_TRAF_RT_NO = ProbDat[~ProbDat.TRAF_RT_NO.isna()]
On_1 = ["FileName", "Name"]
Prob_LANE_CNT = Prob_LANE_CNT.merge(Prob_TOTAL_WIDT, on=On_1, how="inner")

OutFi = "ProcessedData/Fig/" + "ProbRows" + ".xlsx"
writer = pd.ExcelWriter(OutFi)
for key, val in ProbDict.items():
    val.to_excel(writer, key, na_rep="-", index=False)
writer.save()
# ****************************************************************************************************************************


# 4 Fix issues and group data
# ****************************************************************************************************************************
# NumDuplicates = np.empty(0)

# feature = 'TOTAL_WIDT'

# MainData = GetVariableSummary("I_78_EB_1.xls",Features)
# TempData = MainData["TOTAL_WIDT"]["Ret2"].reset_index()
# Tp2 = Clean_Tot_Width_1stLevel(TempData,feature)
# Tp2 = pd.DataFrame(Tp2).reset_index()
# Tp2.rename(columns = {0:"AADT"},inplace=True)
# Tp2.merge(TempData,left_on= ['Name','AADT'],
#           right_on =['Name','CUR_AADT'],how ='left')
# 4.1 Prelim Checks
# ****************************************************************************************************************************


NumDuplicates = defaultdict(list)
CleanData_Dict = {}

Features = [
    "TRAF_RT_NO",
    "TRAF_RT__1",
    "DISTRICT_N",
    "JURIS",
    "ST_RT_NO",
    "CTY_CODE",
    "DIR_IND",
    "FAC_TYPE",
    "URBAN_RURA",
    "TOTAL_WIDT",
    "LANE_CNT",
    "DIVSR_WIDT",
    "DIVSR_TYPE",
    "CUR_AADT",
    "TRK_PCT",
    "K_FACTOR",
    "D_FACTOR",
    "T_FACTOR",
]
FinAADT_Dat = pd.DataFrame()
FinStRt_Dat = pd.DataFrame()
FinDir_Dat = pd.DataFrame()
FinFacType_Dat = pd.DataFrame()

Features_With_No_Issues = ["ST_RT_NO", "JURIS", "TRAF_RT_NO", "TRAF_RT__1"]


# 5 Clean the data and check for things like local hills and valleys in the AADT data
# ****************************************************************************************************************************
for _, row in FileData.iterrows():
    Fin_Fin_data = pd.DataFrame({"Name": []})
    MainData = GetVariableSummary(row["FileName"], Features)
    for feature in Features:
        temp = np.squeeze(
            MainData[feature]["Ret1"].loc[:, feature].apply(lambda x: len(x)).values
        )
        if feature == "CUR_AADT":
            # Use Max Frequency or Maximum AADT. Also look for valleys and Peaks
            TempData = MainData["CUR_AADT"]["Ret2"].reset_index()
            Tp2 = CleanAADT_1stLevel(TempData)
            if row["FileName"] in ProbDict["CUR_AADT"].FileName.values:
                ""
                Path("ProcessedData/Fig/Clean_{}/".format(feature)).mkdir(
                    parents=True, exist_ok=True
                )
                # PlotlyDebugFigs_2(TempData,Tp2['OutDat'],np.max(temp),feature, row['SheetName'], "ProcessedData/Fig/Clean_{}/Cl_".format(feature))
            NumDuplicates[feature] = NumDuplicates[feature] + Tp2["CountDat"].tolist()
            # FinAADT_Dat = pd.concat([FinAADT_Dat, Tp2['OutDat']])
            CleanData_Dict[feature] = Tp2["OutDat"]
        elif feature in ["CTY_CODE", "DISTRICT_N"]:
            # Check for max freq. For equal freq use previous value
            TempData = MainData[feature]["Ret2"].reset_index()
            Tp2 = CleanCityCode_1stLevel(TempData, feature)
            CleanData_Dict[feature] = Tp2["OutDat"]
            if row["FileName"] in ProbDict[feature].FileName.values:
                Path("ProcessedData/Fig/Clean_{}/".format(feature)).mkdir(
                    parents=True, exist_ok=True
                )
                # PlotlyDebugFigs_2(TempData,Tp2['OutDat'],np.max(temp),feature, row['SheetName'], "ProcessedData/Fig/Clean_{}/Cl_".format(feature))
                ""
            NumDuplicates[feature] = NumDuplicates[feature] + Tp2["CountDat"].tolist()
        elif feature == "DIR_IND":
            # Remove "B". Just use cardinal directions
            TempData = MainData["DIR_IND"]["Ret2"].reset_index()
            CleanData_Dict[feature] = TempData[
                TempData.DIR_IND != "B"
            ]  # ProbDat shows that the only issue with Dir in "B". Just remove it
            FinDir_Dat = pd.concat([FinDir_Dat, CleanData_Dict[feature]])
        elif feature == "FAC_TYPE":
            # I manually check. We need to keep 2 and drop 1
            TempData = MainData["FAC_TYPE"]["Ret2"].reset_index()
            CleanData_Dict[feature] = (
                TempData.groupby("Name")["FAC_TYPE"].max().reset_index()
            )
            FinFacType_Dat = pd.concat([FinFacType_Dat, CleanData_Dict[feature]])
        elif (feature == "TOTAL_WIDT") | (feature == "LANE_CNT"):
            # Use min lane width or Num lanes
            TempData = MainData[feature]["Ret2"].reset_index()
            Tp2 = Clean_Tot_Width_1stLevel(TempData, feature)
            CleanData_Dict[feature] = Tp2["OutDat"]
            NumDuplicates[feature] = NumDuplicates[feature] + Tp2["CountDat"].tolist()
            if row["FileName"] in ProbDict[feature].FileName.values:
                ""
                Path("ProcessedData/Fig/Clean_{}/".format(feature)).mkdir(
                    parents=True, exist_ok=True
                )
                # PlotlyDebugFigs_2(TempData,Tp2['OutDat'],np.max(temp), feature, row['SheetName'], "ProcessedData/Fig/Clean_{}/Cl_".format(feature))
        elif feature in [
            "DIVSR_WIDT",
            "DIVSR_TYPE",
            "URBAN_RURA",
            "TRK_PCT",
            "K_FACTOR",
            "D_FACTOR",
            "T_FACTOR",
        ]:
            # Use the max freq or the max value
            TempData = MainData[feature]["Ret2"].reset_index()
            Tp2 = CleanDivsrType_1stLevel(TempData, feature)
            CleanData_Dict[feature] = Tp2["OutDat"]
            NumDuplicates[feature] = NumDuplicates[feature] + Tp2["CountDat"].tolist()
            if row["FileName"] in ProbDict[feature].FileName.values:
                ""
                Path("ProcessedData/Fig/Clean_{}/".format(feature)).mkdir(
                    parents=True, exist_ok=True
                )
                # PlotlyDebugFigs_2(TempData,Tp2['OutDat'],np.max(temp), feature, row['SheetName'], "ProcessedData/Fig/Clean_{}/Cl_".format(feature))
        else:
            ""
        # No processing needed
        if feature not in Features_With_No_Issues:
            CleanData_Dict[feature] = CleanData_Dict[feature][["Name", feature]]
            Fin_Fin_data = Fin_Fin_data.merge(
                CleanData_Dict[feature], on="Name", how="right"
            )
            TempData1 = MergeMultipleData(MainData, Features_With_No_Issues)
    # TempData1 = MainData['ST_RT_NO']["Ret2"].reset_index()
    # TempData2 = MainData['JURIS']["Ret2"].reset_index()
    # TempData1 = TempData1[['Name','ST_RT_NO']]
    # TempData2 = TempData2[['Name','JURIS']]
    # l_on = ['Name']
    # TempData1 = TempData1.merge(TempData2, on = l_on, how= 'inner')
    # FinStRt_Dat = pd.concat([FinStRt_Dat,TempData1])
    l_on = ["Name"]
    Fin_Fin_data = Fin_Fin_data.merge(TempData1, on=l_on, how="inner")

    OutFi = "ProcessedData/Prcsd_" + row["SheetName"] + ".xlsx"
    writer = pd.ExcelWriter(OutFi)
    Fin_Fin_data.to_excel(writer, row["SheetName"], na_rep="-", index=False)
    writer.save()

for key, val in NumDuplicates.items():
    unique, counts = np.unique(val, return_counts=True)
    print(key)
    print("Unique: ", unique)
    print("Count: ", counts)
    print("-" * 50)
FinFacType_Dat.shape
FinDir_Dat.shape
FinDir_Dat.groupby("DIR_IND").count()
