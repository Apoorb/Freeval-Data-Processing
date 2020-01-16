# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 08:56:17 2020

@author: abibeka
"""
import pandas as pd
import numpy as np
# Change the default stacking
from plotly.offline import plot
import plotly.graph_objects as go
from plotly.subplots import make_subplots



#****************************************************************************************************************************
def GetVariableSummary(FileNm, Features =['CUR_AADT']):
    '''
    

    Parameters
    ----------
    FileNm : str
        Filename for the data.
    Features : List of features, optional
        DESCRIPTION. The default is ['CUR_AADT']. Features that need to be analyzed

    Returns
    -------
    RetDict: Returns a dictionary per feature. 

    '''
    RetDict = {}
    for Feature in Features:
        x1 = pd.ExcelFile(FileNm)
        sheetNm = x1.sheet_names[0]
        dat = x1.parse(sheetNm)
        #Get a list of unique features 
        ReturnDat = pd.Series(dat.groupby(['Name'])[Feature].unique())
        ReturnDat = pd.DataFrame(ReturnDat)
        #Get frequency count of Name, AADT combos. Long data
        ReturnDat2 = dat[['Name',Feature]].groupby(['Name',Feature],as_index=False).size()
        ReturnDat2 = pd.DataFrame(ReturnDat2).reset_index()
        ReturnDat2.columns = ['Name',Feature,'ObsFreq']
        ReturnDat2.loc[:,'UniqNo'] = np.hstack(ReturnDat2.groupby(['Name'])[Feature].apply(lambda x: np.arange(1,len(x)+1)).values)
        ReturnDat2.loc[:,'FreevalSeg'] =  (ReturnDat2.Name - np.min(ReturnDat2.Name)).apply(lambda x: "Seg{}".format(x+1))
        ReturnDat2.set_index(['FreevalSeg','UniqNo'],inplace=True)  
        tempDict = {'Ret1': ReturnDat, 'Ret2':ReturnDat2}
        RetDict[Feature] = tempDict
    return(RetDict)


def GetProbData(data):
    '''
    Parameters
    ----------
    data : Ret2 data from Dictionary of summary data per feature
    feature : str
        feature like CUR_AADT etc.

    Returns
    -------
    ProbRows : Rows with issue

    '''
    #PlotlyDebugFigs(DatDict['Ret2'], row['SheetName'],"ProcessedData/Fig/{}/".format(feature))
    temp = np.squeeze(data.loc[:,feature].apply(lambda x: len(x)).values)
    # NumDuplicates = np.append(NumDuplicates,temp)
    if(np.max(temp)>=2):
        #NumVal = Number of duplicates. Can be 1 meaning no duplicate
        data.loc[:,'NumVal'] = data.loc[:,feature].apply(lambda x: len(x)).values
        data.reset_index(inplace=True)
        ProbRows = data.loc[(data.NumVal >=2),['Name',feature]]
        ProbRows.loc[:,"FeatureNm"] = feature
        ProbRows.loc[:,"NumDuplicates"] = ProbRows.loc[:,feature].apply(lambda x: len(x))
        ProbRows.loc[:,'FileName'] = row['FileName']
        return(ProbRows)
        

def CleanAADT_1stLevel(TempData):
    '''
    Parameters
    ----------
    Temp : pd.DataFrame
        RetDict[CUR_AADT]['Ret2'].
    Returns
    -------
    Single AADT values from multiple values.

    '''
    Tp2 = TempData.groupby(['Name']).apply(CleanAADT_2ndLevel)
    Tp2 = pd.DataFrame(Tp2).reset_index()
    Tp2.rename(columns = {0:"CUR_AADT"},inplace=True)
    Tp2 = Tp2.merge(TempData,left_on= ['Name','CUR_AADT'],
              right_on =['Name','CUR_AADT'],how ='left')
    Tp2.sort_values("Name")
    Tp2.reset_index(inplace=True,drop=True)
    Tp2.UniqNo = 1
    Tp2.loc[:,'AADT_shift'] = Tp2.CUR_AADT.shift(1) #---------@@@@@@@-----
    Tp2.loc[:,'DeltaPrevObs'] = Tp2.CUR_AADT.diff(-1)
    Tp2.loc[:,'DeltaNextObs'] = Tp2.loc[:,'DeltaPrevObs'].shift(1)
    Tp2.loc[:,'DeltaPrevObs'] = Tp2.loc[:,'DeltaPrevObs'] * -1
    mask = ((Tp2.DeltaPrevObs>20000) & (Tp2.DeltaNextObs>20000)) |((Tp2.DeltaPrevObs<-20000) & (Tp2.DeltaNextObs<-20000))
    Tp2.loc[Tp2.index[mask],'CUR_AADT'] = Tp2.loc[Tp2.index[mask],'AADT_shift']
    CountData = Tp2.groupby(['Name'])['CUR_AADT'].count().values
    return({'OutDat' : Tp2, 'CountDat': CountData})

def CleanAADT_2ndLevel(data):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        RetDict[CUR_AADT]['Ret2'].
    Returns
    -------
    Single AADT values from multiple values.

    '''
    if(np.std(data.ObsFreq) ==0):
        AADT = data.CUR_AADT.max()
    else:
        AADT = data.loc[data.ObsFreq==max(data.ObsFreq),'CUR_AADT'].values[0]
    return(AADT)


def CleanCityCode_1stLevel(TempData):
    '''
    Parameters
    ----------
    Temp : pd.DataFrame
        RetDict[CTY_CODE]['Ret2'].
    Returns
    -------
    Single AADT values from multiple values.

    '''
    Tp2 = TempData.groupby(['Name']).apply(CleanCityCode_2ndLevel).reset_index()
    Tp2.rename(columns = {0:"CTY_CODE"},inplace=True)
    Tp2['CTY_CODE'] = Tp2['CTY_CODE'].fillna(method ='bfill')
    Tp2 = Tp2.merge(TempData,left_on= ['Name','CTY_CODE'],
              right_on =['Name','CTY_CODE'],how ='left')
    Tp2.sort_values("Name")
    Tp2.reset_index(inplace=True,drop=True)
    Tp2.UniqNo = 1
    #Debug_Dat = TempData.groupby(['Name'])['ObsFreq']
    #data = Debug_Dat.get_group(100000830078)
    CountData = Tp2.groupby(['Name'])['CTY_CODE'].count().values
    # Tp2 = Tp2[['Name','CTY_CODE']]
    return({'OutDat' : Tp2, 'CountDat': CountData})

def CleanCityCode_2ndLevel(data):
    '''
    Parameters
    ----------
    data : list from a row of pd.DataFrame 
        RetDict[CTY_CODE]['Ret2'].
    Returns
    -------
    Single CTY_CODE values from multiple values.

    '''
    if(np.std(data.ObsFreq) ==0) & (data.ObsFreq.size>1):
        CtyCode = np.nan
    else:
        CtyCode = data.loc[data.ObsFreq==max(data.ObsFreq),'CTY_CODE'].values[0]
    return(CtyCode)




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

def PlotlyDebugFigs_2(Dat_Plot_Pre, Dat_Plot_Post, SheetNm, feature ,OutPath = 'ProcessedData/Fig/Junk/'):
    '''
    Draw plotly figures based on Dat_Plot
    '''
    fig = make_subplots(rows =2, cols =1, shared_xaxes=True,
                        subplot_titles=("Before {}".format(SheetNm),"After {}".format(SheetNm)))
    for grp ,data in enumerate([Dat_Plot_Pre, Dat_Plot_Post]):
        if grp == 0:
            Legend1 = "Before"
        else: Legend1 = "After"
        for UnqNo, Label in enumerate(["1st {} and Freq".format(feature),"2nd {} and Freq".format(feature),"3rd {} and Freq".format(feature)]):
            dat_sub = data[data.UniqNo== UnqNo + 1]
            fig.add_trace(go.Bar(name = Label, x=dat_sub["FreevalSeg"], y=dat_sub[feature], text = dat_sub["ObsFreq"],legendgroup=Legend1),row=grp+1,col=1)
        # Here we modify the tickangle of the xaxis, resulting in rotated labels.
    fig.update_layout(barmode='group', xaxis_tickangle=-45)
    plot(fig,filename=OutPath+"{}_{}.html".format(feature, SheetNm), auto_open=False)
    return()

#****************************************************************************************************************************
