# -*- coding: utf-8 -*-
"""
Created on Wed May 20 13:01:42 2020

@author: abibeka
"""

# 0.0 Housekeeping. Clear variable space
#******************************************************************************************
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()


import os
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import pandasql as ps

OuterDir= r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Passive Projects\Freeval-PA\May_20_2020'
Part1North = "Part1-RunningSum_CurveCycle/Part1-RunningSum_NorthCurveCycle.xlsx"
Par1South = "Part1-RunningSum_CurveCycle/Part1-RunningSum_SouthCurveCycle.xlsx"
Par2North = "Part2-RunningSum/Part2-RunningSum_North.xlsx"
Par2South = "Part2-RunningSum/Part2-RunningSum_South.xlsx"
PathList = [Part1North,Par1South,Par2North,Par2South]

TempDatList = []
for path in PathList:
    dat = pd.read_excel(os.path.join(OuterDir,path),converters={'FCOUNTY':str,'FROUTE':str,'FSEG':str,'Concat':str})
    dat.drop(columns="RunningSum",inplace=True)
    dat.sort_values(by=["FCOUNTY","FROUTE","FSEG","FOFFSET"],inplace=True)
    dat.loc[:,'RunningSum']= dat.groupby(["FCOUNTY","FROUTE"])['FLen_Cor'].cumsum()
    dat.loc[:,"RunSumStart"] = dat.groupby(["FCOUNTY","FROUTE"])['RunningSum'].shift().fillna(0)
    dat.loc[:,'RunSumEnd'] = dat.RunningSum
    dat.loc[:,'CheckLen'] = dat.RunSumEnd - dat.RunSumStart
    assert(sum(~(dat.CheckLen == dat.FLen_Cor))==0)
    TempDatList.append(dat)
FinDat = pd.concat(TempDatList) 
FinDat.drop(columns='CheckLen',inplace=True)
FinDat.to_csv(os.path.join(OuterDir,'Part1Part2RunningSumStartEndTags.csv'))


checkPart1N = TempDatList[0].head(10000)
checkPart1S = TempDatList[1].head(10000)
checkPart2N = TempDatList[2].head(10000)
checkPart2S = TempDatList[3].head(10000)



NLP_dat = gpd.read_file(os.path.join(OuterDir,'Grade Data','NLFID_Table.shx'))
NLP_dat = NLP_dat[['CTY_CODE','ST_RT_NO','NLF_CNTL_B','NLF_CNTL_E','NLF_ID','SEG_NO']]
NLP_dat.SEG_NO.value_counts()
NLP_dat.SEG_NO =NLP_dat.SEG_NO.astype(int)
NLP_dat.loc[:,"FDIR"] = NLP_dat.SEG_NO.apply(lambda x: "N" if x % 2 == 0 else "S")



FinDat.FCOUNTY = FinDat.FCOUNTY.astype(int)
FinDat.FROUTE = FinDat.FROUTE.astype(int)
NLP_dat.CTY_CODE = NLP_dat.CTY_CODE.astype(int)
NLP_dat.ST_RT_NO = NLP_dat.ST_RT_NO.astype(int)
NLP_dat.loc[:,'NLPSegBins'] = pd.IntervalIndex.from_arrays(NLP_dat.NLF_CNTL_B,NLP_dat.NLF_CNTL_E,'left') 

NLP_datGrouped = NLP_dat.groupby(['CTY_CODE','ST_RT_NO','FDIR'])
FinDatGrouped = FinDat.groupby(['FCOUNTY','FROUTE','FDIR'])

TempList1 =[]
for name, group in FinDatGrouped:
    try:
        NLP_1= NLP_datGrouped.get_group(name)
        print(name)
        group.loc[:,'NLPBinRunStart'] = pd.cut(group.RunSumStart,pd.IntervalIndex(NLP_1.NLPSegBins))
        group.loc[:,'NLPBinRunEnd'] = pd.cut(group.RunSumEnd,pd.IntervalIndex(NLP_1.NLPSegBins))
        TempList1.append(group)
    except:
        ""
NLPbinsDat = pd.concat(TempList1)

NLPbinsDat = NLPbinsDat[["FCOUNTY","FROUTE","FSEG","FOFFSET",'NLPBinRunStart','NLPBinRunEnd']]

FinDat2 = FinDat.merge(NLPbinsDat,on=["FCOUNTY","FROUTE","FSEG","FOFFSET"],how="left")
NLP_dat = NLP_dat[['CTY_CODE','ST_RT_NO','FDIR','NLPSegBins','NLF_ID']]
RightCom = ['CTY_CODE','ST_RT_NO','FDIR','NLPSegBins']
leftCom = ["FCOUNTY","FROUTE","FDIR",'NLPBinRunStart']

FinDat2 = FinDat2.merge(NLP_dat, left_on =leftCom, right_on = RightCom,how="left" )
Check = FinDat2.head(60000)

FinDat2.to_csv(os.path.join(OuterDir,'TestingNLP_ID_Merge_Part1Part2RunningSumStartEndTags.csv'))




