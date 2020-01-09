# -*- coding: utf-8 -*-
"""
Created on Thu Jan  9 08:44:45 2020

@author: abibeka
"""


#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()



import os
import pandas as pd
import numpy as np
import subprocess 
import glob
# Change the default stacking
from plotly.offline import plot
import plotly.graph_objects as go
import plotly.express as px

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

#****************************************************************************************************************************
def GetAADTsummary(FileNm):
    x1 = pd.ExcelFile(FileNm)
    sheetNm = x1.sheet_names[0]
    dat = x1.parse(sheetNm)
    ReturnDat = pd.Series(dat.groupby(['Name'])['CUR_AADT'].unique())
    ReturnDat = pd.DataFrame(ReturnDat)
    ReturnDat.loc[:,'AllVals'] = pd.Series(dat.groupby(['Name'],as_index=False)['CUR_AADT'].apply(list))
    ReturnDat2 = dat[['Name','CUR_AADT']].groupby(['Name','CUR_AADT'],as_index=False).size()
    ReturnDat2 = pd.DataFrame(ReturnDat2).reset_index()
    ReturnDat2.columns = ['Name','CUR_AADT','ObsFreq']
    ReturnDat2.loc[:,'UniqNo'] = np.hstack(ReturnDat2.groupby(['Name'])['CUR_AADT'].apply(lambda x: np.arange(1,len(x)+1)).values)
    ReturnDat2.loc[:,'FreevalSeg'] =  (ReturnDat2.Name - np.min(ReturnDat2.Name)).apply(lambda x: "Seg{}".format(x+1))
    ReturnDat2.set_index(['FreevalSeg','UniqNo'],inplace=True)  
    RetDict = {'Ret1': ReturnDat, 'Ret2':ReturnDat2}
    return(RetDict)

def PlotlyDebugFigs(Dat_Plot, SheetNm,OutPath = 'ProcessedData/Fig/Junk/'):
    '''
    Draw plotly figures based on Dat_Plot
    '''
    dat_1stObs = Dat_Plot[Dat_Plot.UniqNo==1]
    dat_2ndObs = Dat_Plot[Dat_Plot.UniqNo==2]
    dat_3rdObs = Dat_Plot[Dat_Plot.UniqNo==3]
    fig = go.Figure(data = [
        go.Bar(name = "1st AADT and Freq", x=dat_1stObs["FreevalSeg"], y=dat_1stObs["CUR_AADT"], text = dat_1stObs["ObsFreq"]),
        go.Bar(name = "2nd AADT and Freq", x=dat_2ndObs["FreevalSeg"], y=dat_2ndObs["CUR_AADT"],text = dat_2ndObs["ObsFreq"]),
        go.Bar(name = "3rd AADT and Freq", x=dat_3rdObs["FreevalSeg"], y=dat_3rdObs["CUR_AADT"], text = dat_3rdObs["ObsFreq"])
        ])
    # Here we modify the tickangle of the xaxis, resulting in rotated labels.
    fig.update_layout(barmode='group', xaxis_tickangle=-45)
    plot(fig,filename=OutPath+"AADT_{}.html".format(SheetNm), auto_open=False)
    return()
#****************************************************************************************************************************

AADT_SumDat = pd.DataFrame()
AADT_SumDat2 = pd.DataFrame()

NumDuplicates = np.empty(0)
ProbDat = pd.DataFrame()
for _, row in FileData.iterrows():
    RetDict = GetAADTsummary(row['FileName'])
    RetDict['Ret2'] = RetDict['Ret2'].reset_index()  
    PlotlyDebugFigs(RetDict['Ret2'], row['SheetName'],"ProcessedData/Fig/AADT/")
    RetDict['Ret2'].loc[:,"SiteName"] = row['SheetName']
    temp = np.squeeze(RetDict['Ret1'].CUR_AADT.apply(lambda x: len(x)).values)
    NumDuplicates = np.append(NumDuplicates,temp)
    if(np.max(temp)==3):
        RetDict['Ret1'].loc[:,'NumVal'] = RetDict['Ret1'].CUR_AADT.apply(lambda x: len(x)).values
        RetDict['Ret1'].reset_index(inplace=True)
        ProbRows = RetDict['Ret1'].loc[(RetDict['Ret1'].NumVal ==3),['Name','CUR_AADT']]
        ProbRows.loc[:,'FileName'] = row['FileName']
        ProbDat = pd.concat([ProbDat,ProbRows])
unique, counts = np.unique(NumDuplicates, return_counts=True)
unique
counts
counts.sum()

RetDict['Ret2']
RetDict['Ret1']
def CleanAADT(data):
    if(np.std(data.ObsFreq) ==0):
        AADT = data.CUR_AADT.max()
    else:
        AADT = data.loc[data.ObsFreq==max(data.ObsFreq),'CUR_AADT'].values[0]
    return(AADT)

TempData = GetAADTsummary("I_78_EB_1.xls")["Ret2"].reset_index()
Tp2 = TempData.groupby(['Name']).apply(CleanAADT)
Tp2 = pd.DataFrame(Tp2).reset_index()
Tp2.rename(columns = {0:"AADT"},inplace=True)
Tp2.merge(TempData,left_on= ['Name','AADT'],
          right_on =['Name','CUR_AADT'],how ='left')

NumDuplicates2 = np.empty(0)

for _, row in FileData.iterrows():
    TempData = GetAADTsummary(row['FileName'])["Ret2"].reset_index()
    Tp2 = TempData.groupby(['Name']).apply(CleanAADT)
    Tp2 = pd.DataFrame(Tp2).reset_index()
    Tp2.rename(columns = {0:"AADT"},inplace=True)
    Tp2 = Tp2.merge(TempData,left_on= ['Name','AADT'],
              right_on =['Name','CUR_AADT'],how ='left')
    Tp2.sort_values("Name")
    Tp2.reset_index(inplace=True,drop=True)
    Tp2.UniqNo = 1
    Tp2.loc[:,'AADT_shift'] = Tp2.AADT.shift(1) #---------@@@@@@@-----
    Tp2.loc[:,'DeltaPrevObs'] = Tp2.AADT.diff(-1)
    Tp2.loc[:,'DeltaNextObs'] = Tp2.loc[:,'DeltaPrevObs'].shift(-1)
    Tp2.loc[:,'DeltaPrevObs'] = Tp2.loc[:,'DeltaPrevObs'] * -1
    mask = ((Tp2.DeltaPrevObs>20000) & (Tp2.DeltaNextObs>20000)) |((Tp2.DeltaPrevObs<-20000) & (Tp2.DeltaNextObs<-20000))

    Tp2.loc[Tp2.index[mask],'AADT'] = Tp2.loc[Tp2.index[mask],'AADT_shift']
    PlotlyDebugFigs(Tp2, row['SheetName'],"ProcessedData/Fig/Clean_AADT/Cl_")
    CountData = Tp2.groupby(['Name'])['CUR_AADT'].count().values
    NumDuplicates2 = np.append(NumDuplicates2,CountData)
unique, counts = np.unique(NumDuplicates2, return_counts=True)


# fi ='I_70_EB_1_Border_to_Turnpike.xls'
# RetDict = GetAADTsummary(fi)   
# dat2 = RetDict['Ret2']


OutFi = "ProcessedData/Prcsd_"+fi.replace('.xls', '.xlsx')  
SheetNm = fi.split('.')[0]
writer=pd.ExcelWriter(OutFi)
dat2.to_excel(writer, SheetNm,na_rep='-')
writer.save() 
#****************************************************************************************************************************



AADT_SumDat.loc[:,'NumDuplicates'] = AADT_SumDat.CUR_AADT.apply(lambda x: len(x))
AADT_SumDat.loc[:,'DiffinDuplicates'] = AADT_SumDat.CUR_AADT.apply(lambda x: abs(x[1]-x[0]) if len(x) >1 else 0)

AADT_SumDat2.reset_index(inplace=True)
AADT_SumDat2.loc[:,'UniqNo'] = np.hstack(AADT_SumDat2.groupby(['Name'])['CUR_AADT'].apply(lambda x: np.arange(1,len(x)+1)).values)


AADT_SumDat2.groupby(['Name'])























