# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 15:07:26 2020

#Create summary statistics of the Segmentation data
@author: abibeka
"""
# 1. Import Libraries
# ********************************************************************************************
import pandas as pd
import matplotlib.pyplot as plt
import os
import re

os.chdir(
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\NCHRP7-26\Data\PennDOT-Data"
)

# 2 Read Data. Make some basic changes
# ********************************************************************************************
def RenameSegType(No):
    NumNmDict = {0: "Basic", 1: "OnRamp", 2: "OffRamp", 3: "Overlap", 4: "Weave"}
    return NumNmDict[No]


Dat = pd.read_csv("PennDOT_SegDB_20191203.csv")
Dat.columns
Dat.loc[:, "SegTypNm"] = Dat.segType.apply(lambda x: RenameSegType(x))
sum(Dat.SegTypNm.isna())


# 3 Get the big picture--- Merge/ Diverge/ and Weave
# ********************************************************************************************
TotalLenMi = Dat.segLenFt.sum() / 5280

Dat.segOnrSide.value_counts()  # 0 is right and 1 in left
Dat.segOfrSide.value_counts()  # 0 is right and 1 in left
# Get # of Merge, diverge and Weaving Sections
Dat1 = Dat.SegTypNm.value_counts()
Dat1 = Dat1[~Dat1.index.isin(["Basic", "Overlap"])]
Dat1.index.rename("Type", inplace=True)

Dat1 = Dat1.reset_index()
Dat1.rename(columns={"SegTypNm": "TotFreq"}, inplace=True)
Dat1.loc[:, "FreqPerMi"] = Dat1.TotFreq / TotalLenMi
Dat1.loc[:, "TotalMiles"] = TotalLenMi

Dat2 = Dat.groupby(["SegTypNm", "segNumLanes"])["SegTypNm"].count().unstack()
Dat2 = Dat2[~Dat2.index.isin(["Basic", "Overlap"])]
# Dat2.loc[:,'FreqPerMi'] = Dat2.TotFreq/ TotalLenMi
# Dat2.loc[:,"TotalMiles"] = TotalLenMi
Dat2.sum(axis=1)

# 4 Identify NCHRP 7-26 Ramp Types: Close Merge and Diverge
# ********************************************************************************************
def FindCloseMergeDiverge(Row):
    """
    Check if the length of current ramp <= 1500 ft. 
    If true then check the current row is a merge (diverge) and
    the next row is merge (diverge) 
    When all these conditions are met we get close merge (close diverge)
    """
    retVal = ""
    if Row["segLenFt"] <= 1500:
        if (
            (Row["SegTypNm"] == "OnRamp")
            & (Row["FolSegType"] == "OnRamp")
            & (Row["segOnrSide"] == 0)
            & (Row["FolOnrSide"] == 0)
        ):
            retVal = "Close Merge"
    if Row["FoSegLen"] <= 1500:
        if (
            (Row["SegTypNm"] == "OffRamp")
            & (Row["FolSegType"] == "OffRamp")
            & (Row["segOfrSide"] == 0)
            & (Row["FolOfrSide"] == 0)
        ):
            retVal = "Close Diverge"
    return retVal


# Get the info of next segment
Dat.loc[:, "FolSegType"] = Dat.SegTypNm.shift(-1)
Dat.loc[:, "FoSegLen"] = Dat.segLenFt.shift(-1)
Dat.loc[:, "FolOnrSide"] = Dat.segOnrSide.shift(-1)
Dat.loc[:, "FolOfrSide"] = Dat.segOfrSide.shift(-1)
# Indentify # of close merge and diverge
Dat.loc[:, "CloseMerDiv"] = Dat.apply(FindCloseMergeDiverge, axis=1)
Dat.loc[:, "CloseMerDiv"].value_counts()

Dat_ClMerDiv = (
    Dat.groupby(["CloseMerDiv", "segNumLanes"])["CloseMerDiv"].count().unstack()
)
Dat_ClMerDiv = Dat_ClMerDiv[Dat_ClMerDiv.index.isin(["Close Merge", "Close Diverge"])]
# 4 Identify NCHRP 7-26 Ramp Types: Simple and Two lane Merge and Diverge
# ********************************************************************************************
def FindSimple_TwoLaneMergeDiverge(Row):
    """
    Parameters
    ----------
    Row : TYPE
        Row of freeval segmentation database
        Would need some information about the previous ramp. To avoid double counting Close M/D
    Returns
    -------
    The type of ramp : 2 Lane, close M/D or None.
    """
    retVal = ""
    if Row["CloseMerDiv"] not in ["Close Merge", "Close Diverge"]:
        if (
            (Row["SegTypNm"] == "OnRamp")
            & (Row["segOnrNumLanes"] == 2)
            & (Row["segOnrSide"] == 0)
        ):
            retVal = "Two Lane On-Ramp"
        elif (
            (Row["SegTypNm"] == "OffRamp")
            & (Row["segOfrNumLanes"] == 2)
            & (Row["segOfrSide"] == 0)
        ):
            retVal = "Two Lane Off-Ramp"
        elif (
            (Row["SegTypNm"] == "OnRamp")
            & (Row["segOnrNumLanes"] == 1)
            & (Row["segOnrSide"] == 0)
            & (Row["FolNumLanes"] == Row["segNumLanes"])
        ):
            retVal = "Simple Merge"
        elif (
            (Row["SegTypNm"] == "OffRamp")
            & (Row["segOfrNumLanes"] == 1)
            & (Row["segOfrSide"] == 0)
            & (Row["FolNumLanes"] == Row["segNumLanes"])
        ):
            retVal = "Simple Diverge"
        elif (Row["SegTypNm"] == "OnRamp") & (Row["segOnrSide"] == 1):
            retVal = "Left Merge"
        elif (Row["SegTypNm"] == "OffRamp") & (Row["segOfrSide"] == 1):
            retVal = "Left Diverge"
    return retVal


Dat.loc[:, "FolNumLanes"] = Dat.segNumLanes.shift(-1)

Dat.loc[:, "Simple_2Ln_MD"] = Dat.apply(FindSimple_TwoLaneMergeDiverge, axis=1)
Dat.apply(FindSimple_TwoLaneMergeDiverge, axis=1).value_counts()

Dat_Multi_RampTy = (
    Dat.groupby(["Simple_2Ln_MD", "segNumLanes"])["Simple_2Ln_MD"].count().unstack()
)
Dat_Multi_RampTy = Dat_Multi_RampTy[~Dat_Multi_RampTy.index.isin([""])]

# 4 Identify NCHRP 7-26 Ramp Types: Lane drop and add diverge/ merge
# ********************************************************************************************


def Find_LaneAdd_DropMerge(Row):
    """
    Check if the acc/decc lane is same length as the segment or if the acc/decc lane length is greater than 1500 (almost 1500 works too!)
    """
    retVal = ""
    if (
        (Row["SegTypNm"] == "OnRamp")
        & (Row["FolNumLanes"] == Row["segNumLanes"] + 1)
        & (Row.segAccDecLen >= Row.segLenFt - 50)
        & (Row["segAccDecLen"] >= 1500)
    ):
        retVal = "LaneAddMerge"
    elif (
        (Row["SegTypNm"] == "OffRamp")
        & (Row["FolNumLanes"] == Row["segNumLanes"] - 1)
        & (Row.segAccDecLen >= Row.segLenFt - 50)
        & (Row["segAccDecLen"] >= 1500)
    ):
        retVal = "LaneDropDiverge"
    elif (
        (Row["SegTypNm"] == "OnRamp")
        & (Row["segOnrNumLanes"] >= 2)
        & (
            (Row["FolNumLanes"] == Row["segNumLanes"] + 2)
            & (Row.segAccDecLen >= Row.segLenFt - 50)
        )
    ):
        retVal = "Major Merge"
    elif (
        (Row["SegTypNm"] == "OffRamp")
        & (Row["segOfrNumLanes"] >= 2)
        & (
            (Row["FolNumLanes"] == Row["segNumLanes"] - 2)
            & (Row.segAccDecLen >= Row.segLenFt - 50)
        )
    ):
        retVal = "Major Diverge"
    return retVal


Dat.loc[:, "LaneAdd_Drop"] = Dat.apply(Find_LaneAdd_DropMerge, axis=1)
Dat.apply(Find_LaneAdd_DropMerge, axis=1).value_counts()

Dat_LnAddDrp = (
    Dat.groupby(["LaneAdd_Drop", "segNumLanes"])["LaneAdd_Drop"].count().unstack()
)
Dat_LnAddDrp = Dat_LnAddDrp[~Dat_LnAddDrp.index.isin([""])]


Dat2
DatSum1 = pd.concat([Dat_ClMerDiv, Dat_Multi_RampTy, Dat_LnAddDrp])
DatSum1 = DatSum1.loc[
    [
        "Simple Merge",
        "Simple Diverge",
        "Close Merge",
        "Close Diverge",
        "Lane Add Merge",
        "Lane Drop Diverge",
        "Two Lane On-Ramp",
        "Two Lane Off-Ramp",
        "Left Merge",
        "Left Diverge",
    ]
]
# Dat.to_csv('Debug.csv')

PatLat = "\((\d+.\d+)l"
PatLong = "l(.?-\d+.\d+)\)"

Dat.loc[:, "Lat"] = Dat.segPolyLine.str.extract(PatLat).values
Dat.loc[:, "Long"] = Dat.segPolyLine.str.extract(PatLong).values
# st = "(34.18571/ -78.08056)l(34.187870000000004/-78.08056)"
# re.search('\((\d+.\d+)/',st).group(1)
# re.search('/(-\d+.\d+)\)',st).group(1)


DatComb = pd.concat([Dat_ClMerDiv, Dat_LnAddDrp, Dat_Multi_RampTy])
writer = pd.ExcelWriter("PennDOT-Processed-Data.xlsx")
Dat2.to_excel(writer, "SummaryAll")
DatComb.to_excel(writer, "Bundle3-Sum")
Dat.to_excel(writer, "ProcessedData")
writer.save()
# ********************************************************************************************


Dat2 = Dat[Dat.segLenFt == Dat.segAccDecLen]

sum(Dat.segLenFt == Dat.segAccDecLen)
sum(Dat.segOnrNumLanes == 2)
sum(Dat.segOfrNumLanes == 2)
