# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 08:55:16 2020

@author: abibeka
"""

import os
import pandas as pd
import numpy as np
import subprocess 
import glob
import sys
sys.path.append(os.path.abspath(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\Freeval-Data-Processing"))
from CommonFunctions_FreevalPA_Cleaning import GetVariableSummary
from CommonFunctions_FreevalPA_Cleaning import GetProbData
from CommonFunctions_FreevalPA_Cleaning import CleanAADT_1stLevel
from CommonFunctions_FreevalPA_Cleaning import CleanAADT_2ndLevel
from CommonFunctions_FreevalPA_Cleaning import CleanCityCode_1stLevel
from CommonFunctions_FreevalPA_Cleaning import CleanCityCode_2ndLevel
from CommonFunctions_FreevalPA_Cleaning import PlotlyDebugFigs_2


#1 Get all the files. Store file and sheetname in a dataframe. Make sure 1 sheet per file
#****************************************************************************************************************************
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Passive Projects\Freeval-PA\Intersected_Tables_XLS')
ListFiles = glob.glob('*.xls')
# Understand the file name and the sheet name
FileData = pd.DataFrame()
for file in ListFiles:
    x1 = pd.ExcelFile(file)
    assert len(x1.sheet_names)==1, "More than 1 sheet"
    temp = pd.DataFrame({'FileName':file,'SheetName':x1.sheet_names})
    FileData = pd.concat([FileData,temp])
FileData.reset_index(drop=True,inplace=True)

#Sample
file = ListFiles[0]
x1 = pd.ExcelFile(file)
x1.parse().columns
#****************************************************************************************************************************


#2 Get summary of duplicates in the data 
#****************************************************************************************************************************
# NumDuplicates = np.empty(0)
ProbDat = pd.DataFrame()
NumRowsDat = pd.DataFrame()
Features = ['CUR_AADT','ST_RT_NO', 'CTY_CODE']
Features_RetDict = GetVariableSummary('I_80_EB_1.xls',Features)


for _, row in FileData.iterrows():
    Features_RetDict = GetVariableSummary(row['FileName'],Features)
    #Get the # of unique Names per file 
    TempDict_0 = {"Name" : [row['FileName']], "Rows" : [Features_RetDict['CUR_AADT']['Ret1'].shape[0]]}
    Temp_0 = pd.DataFrame.from_dict(TempDict_0)
    NumRowsDat = pd.concat([NumRowsDat,Temp_0])
    #For each feature get the # of duplicates 
    for feature in Features:
        DatDict = Features_RetDict[feature]
        ProbRows = GetProbData(DatDict['Ret1'])
        ProbDat = pd.concat([ProbDat,ProbRows])
#****************************************************************************************************************************


#3 Look at the problem files and type of issues 
#****************************************************************************************************************************
# NumDuplicates = np.empty(0)
ProbDat.columns
ProbDatSum = ProbDat.groupby(['FileName','FeatureNm','NumDuplicates']).agg({'NumDuplicates':{'Tot':'size'}})
ProbDatSum.columns = ProbDatSum.columns.droplevel(0)
ProbDatSum = ProbDatSum.reset_index()
ProbDict = {}
for feature in Features:
    ProbDict[feature]  = ProbDatSum[ProbDatSum.FeatureNm==feature]
ProbDict['CUR_AADT'].Tot.sum()
#****************************************************************************************************************************


#4 Fix issues and group data 
#****************************************************************************************************************************
# NumDuplicates = np.empty(0)




#****************************************************************************************************************************


# Clean the data and check for things like local hills and valleys in the AADT data
NumDuplicates_AADT = np.empty(0)
NumDuplicates_CTY_Code = np.empty(0)
Features = ['CUR_AADT','ST_RT_NO', 'CTY_CODE']
FinAADT_Dat = pd.DataFrame()
#****************************************************************************************************************************
for _, row in FileData.iterrows():
    MainData = GetVariableSummary(row['FileName'],Features)
    for feature in Features:
        if(feature == "CUR_AADT"):
            TempData = MainData["CUR_AADT"]["Ret2"].reset_index()
            CleanDat_AADT = CleanAADT_1stLevel(TempData)
            if(row['FileName'] in ProbDict['CUR_AADT'].FileName.values):
                PlotlyDebugFigs_2(TempData,CleanDat_AADT['OutDat'], row['SheetName'],feature, "ProcessedData/Fig/Clean_AADT/Cl_")
            NumDuplicates_AADT = np.append(NumDuplicates_AADT,CleanDat_AADT['CountDat'])
            FinAADT_Dat = pd.concat([FinAADT_Dat, CleanDat_AADT['OutDat']])
        elif(feature =="CTY_CODE"):
            TempData = MainData['CTY_CODE']["Ret2"].reset_index()
            CleanDat_CITY_CODE = CleanCityCode_1stLevel(TempData)
            if(row['FileName'] in ProbDict['CTY_CODE'].FileName.values):
                #PlotlyDebugFigs_2(TempData,CleanDat_CITY_CODE['OutDat'], row['SheetName'],feature, "ProcessedData/Fig/Clean_CtyCode/Cl_")
                ""
            NumDuplicates_CTY_Code = np.append(NumDuplicates_CTY_Code,CleanDat_CITY_CODE['CountDat'])
        else: ""
    # OutFi = "ProcessedData/Prcsd_"+row['SheetName']+'.xlsx'
    # writer=pd.ExcelWriter(OutFi)
    # CleanDat['OutDat'].to_excel(writer, row['SheetName'],na_rep='-')
    # writer.save() 

FinAADT_Dat.shape
unique, counts = np.unique(NumDuplicates_AADT, return_counts=True)
unique2, counts2 = np.unique(NumDuplicates_CTY_Code, return_counts=True)
