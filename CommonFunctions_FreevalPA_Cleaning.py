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


def GetProbData(data, feature, FiName):
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
    temp = np.squeeze(data.loc[:,feature].apply(lambda x: len(x)).values)
    if(np.max(temp)>=2):
        #Plot here so that you don't have too many plots
        #NumVal = Number of duplicates. Can be 1 meaning no duplicate
        data.loc[:,'NumVal'] = data.loc[:,feature].apply(lambda x: len(x)).values
        data.reset_index(inplace=True)
        ProbRows = data.loc[(data.NumVal >=2),['Name',feature]]
        ProbRows.loc[:,"FeatureNm"] = feature
        ProbRows.loc[:,"NumDuplicates"] = ProbRows.loc[:,feature].apply(lambda x: len(x))
        ProbRows.loc[:,'FileName'] = FiName
        return(ProbRows)
        

def MergeMultipleData(MainData, Features):
    '''
    Parameters
    ----------
    MainData : Dict of dataframes
        Summary data for different features .
    Features : TYPE
        Different features to merge togather. These features don't need processing.

    Returns
    -------
    FinDat : Single data with all the features .

    '''
    FinData = pd.DataFrame({'Name':[]})
    l_on = ['Name'] 
    for feature in Features:
        TempData1 = MainData[feature]['Ret2'].reset_index()
        TempData1 = TempData1[['Name',feature]]
        FinData = FinData.merge(TempData1, on = l_on, how= 'right')
    return(FinData)

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


def CleanCityCode_1stLevel(TempData, feature):
    '''
    Parameters
    ----------
    Temp : pd.DataFrame
        RetDict[CTY_CODE]['Ret2'].
    Made it generic for other features
    Returns
    -------
    Single AADT values from multiple values.

    '''
    Tp2 = TempData.groupby(['Name']).apply(CleanCityCode_2ndLevel,feature).reset_index()
    Tp2.rename(columns = {0:feature},inplace=True)
    Tp2[feature] = Tp2[feature].fillna(method ='bfill')
    Tp2 = Tp2.merge(TempData,left_on= ['Name',feature],
              right_on =['Name',feature],how ='left')
    Tp2.sort_values("Name")
    Tp2.reset_index(inplace=True,drop=True)
    Tp2.UniqNo = 1
    #Debug_Dat = TempData.groupby(['Name'])['ObsFreq']
    #data = Debug_Dat.get_group(100000830078)
    CountData = Tp2.groupby(['Name'])[feature].count().values
    # Tp2 = Tp2[['Name','CTY_CODE']]
    return({'OutDat' : Tp2, 'CountDat': CountData})

def CleanCityCode_2ndLevel(data, feature):
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
        Feature_val = np.nan
    else:
        Feature_val = data.loc[data.ObsFreq==max(data.ObsFreq),feature].values[0]
    return(Feature_val)


def CleanTotWidth_2ndLevel(data, feature):
    '''
    Parameters
    ----------
    data : list from a row of pd.DataFrame 
        RetDict[Tot_Width]['Ret2'].
    Returns
    -------
    Single CTY_CODE values from multiple values.

    '''
    if(np.std(data.ObsFreq) ==0) & (data.ObsFreq.size>1):
        Feature_val = data[feature].min()
    else:
        Feature_val = data.loc[data.ObsFreq==max(data.ObsFreq),feature].values[0]
    return(Feature_val)

def Clean_Tot_Width_1stLevel(TempData, feature):
    '''
    Parameters
    ----------
    TempData : pd.DataFrame
        RetDict[CTY_CODE]['Ret2'].
    Made it generic for other features
    Returns
    -------
    Single AADT values from multiple values.

    '''
    Tp2 = TempData.groupby(['Name','FreevalSeg']).apply(CleanTotWidth_2ndLevel,feature).reset_index()
    Tp2.rename(columns = {0:feature},inplace=True)
    Tp2[feature] = Tp2[feature].fillna(method ='bfill')
    Tp2 = Tp2.merge(TempData,left_on= ['Name','FreevalSeg',feature],
              right_on =['Name','FreevalSeg',feature],how ='left')
    Tp2.ObsFreq = Tp2.ObsFreq.fillna(1)
    Tp2.sort_values("Name")
    Tp2.reset_index(inplace=True,drop=True)
    Tp2.UniqNo = 1
    #Debug_Dat = TempData.groupby(['Name'])['ObsFreq']
    #data = Debug_Dat.get_group(100000830078)
    CountData = Tp2.groupby(['Name'])[feature].count().values
    # Tp2 = Tp2[['Name','CTY_CODE']]
    return({'OutDat' : Tp2, 'CountDat': CountData})




def CleanDivsrType_1stLevel(TempData, feature):
    '''
    Parameters
    ----------
    Temp : pd.DataFrame
        RetDict[CTY_CODE]['Ret2'].
    Made it generic for other features
    Returns
    -------
    Single AADT values from multiple values.

    '''
    Tp2 = TempData.groupby(['Name']).apply(CleanDivsrType_2ndLevel,feature).reset_index()
    Tp2.rename(columns = {0:feature},inplace=True)
    Tp2[feature] = Tp2[feature].fillna(method ='bfill')
    Tp2 = Tp2.merge(TempData,left_on= ['Name',feature],
              right_on =['Name',feature],how ='left')
    Tp2.sort_values("Name")
    Tp2.reset_index(inplace=True,drop=True)
    Tp2.UniqNo = 1
    #Debug_Dat = TempData.groupby(['Name'])['ObsFreq']
    #data = Debug_Dat.get_group(100000830078)
    CountData = Tp2.groupby(['Name'])[feature].count().values
    # Tp2 = Tp2[['Name','CTY_CODE']]
    return({'OutDat' : Tp2, 'CountDat': CountData})

def CleanDivsrType_2ndLevel(data, feature):
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
        Feature_val = data[feature].max()
    else:
        Feature_val = data.loc[data.ObsFreq==max(data.ObsFreq),feature].values[0]
    return(Feature_val)



def PlotlyDebugFigs(Dat_Plot, MaxDuplicates, feature, SheetNm, OutPath = 'ProcessedData/Fig/Junk/'):
    '''
    

    Parameters
    ----------
    Dat_Plot : TYPE
        Features_RetDict[feature]['Ret2']. Long data
    MaxDuplicates : TYPE
        DESCRIPTION. Maximum number of duplicates
    SheetNm : TYPE
        DESCRIPTION.
    feature : TYPE
        DESCRIPTION.
    OutPath : TYPE, optional
        DESCRIPTION. The default is 'ProcessedData/Fig/Junk/'.

    Returns
    -------
    None.

    '''
    DuplicateData = {}
    for i in range(1,MaxDuplicates+1):
        DuplicateData[i] = Dat_Plot[Dat_Plot.UniqNo==i]
    fig = go.Figure(data = [
        go.Bar(name = "Duplicate {} {} and Freq".format(i,feature), x=DuplicateData[i]["FreevalSeg"], y=DuplicateData[i][feature], text = DuplicateData[i]["ObsFreq"])
        for i in range(1,MaxDuplicates+1)])
    # Here we modify the tickangle of the xaxis, resulting in rotated labels.
    fig.update_layout(barmode='group', xaxis_tickangle=-45)
    plot(fig,filename=OutPath+"{}_{}.html".format(feature, SheetNm), auto_open=False)
    return()

def PlotlyDebugFigs_2(Dat_Plot_Pre, Dat_Plot_Post,MaxDuplicates, feature, SheetNm ,OutPath = 'ProcessedData/Fig/Junk/'):
    '''
    Draw plotly figures based on Dat_Plot
    '''
    fig = make_subplots(rows =2, cols =1, shared_xaxes=True,
                        subplot_titles=("Before {}".format(SheetNm),"After {}".format(SheetNm)))
    for grp ,data in enumerate([Dat_Plot_Pre, Dat_Plot_Post]):
        if grp == 0:
            Legend1 = "Before"
            for i in range(1,MaxDuplicates+1):
                dat_sub = data[data.UniqNo==i]
                fig.add_trace(go.Bar(name = "Duplicate {} {} and Freq".format(i,feature), x=dat_sub["FreevalSeg"], y=dat_sub[feature], text = dat_sub["ObsFreq"],legendgroup=Legend1),row=grp+1,col=1)
        else: 
            Legend1 = "After"
            for UnqNo, Label in enumerate(["Duplicate 1 {} and Freq".format(feature),"Duplicate 2 {} and Freq".format(feature),"Duplicate 3 {} and Freq".format(feature)]):
                dat_sub = data[data.UniqNo== UnqNo + 1]
                fig.add_trace(go.Bar(name = Label, x=dat_sub["FreevalSeg"], y=dat_sub[feature], text = dat_sub["ObsFreq"],legendgroup=Legend1),row=grp+1,col=1)
        # Here we modify the tickangle of the xaxis, resulting in rotated labels.
    fig.update_layout(barmode='group', xaxis_tickangle=-45)
    plot(fig,filename=OutPath+"{}_{}.html".format(feature, SheetNm), auto_open=False)
    return()

#****************************************************************************************************************************
