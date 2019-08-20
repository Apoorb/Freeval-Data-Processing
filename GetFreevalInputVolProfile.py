# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 10:35:35 2019
@author: abibeka
Purpose: Get Freeval Input Vol Profile
"""

#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

#0.1 Import modules
########################################################################################################################
from datetime import datetime
import os
import subprocess #Need for opening Excel file from Python
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import re

PathTMS = os.path.join(os.path.expanduser('~'),'OneDrive - Kittelson & Associates, Inc',
                         'Documents', '23746 - I-83 TSMO','TMS-VolProfile')


#PathKeyValFi = os.path.join(os.path.expanduser('~'),'OneDrive - Kittelson & Associates, Inc',
#                         'Documents', '23746 - I-83 TSMO','AADT-I-83 V2.xlsx')

#No-Build
PathKeyValFi = os.path.join(os.path.expanduser('~'),'OneDrive - Kittelson & Associates, Inc',
                         'Documents', '23746 - I-83 TSMO','AADT-I-83 No Build.xlsx')

#Get the Key value pair for NB TMS files
NB_KeyVal = pd.read_excel(PathKeyValFi, 'SB-NB-KeyValue', skiprows = 2, usecols = 'C:E',nrows=16)

os.chdir(PathTMS)     
os.getcwd()

########################################################################################################################
def GetVolData(SegNm,fileName,ADT,
               DirNBorSB='NB-TMS',
               PatternFNm= "^(North|East) *Lane *[0-9] *Volume$",
               Debug= True):
    '''
    SegNm: Name of the segment (arbitrary- Can be changed)
    fileName: TMS file corresponding to the above segment name
    ADT: ADT data corresponding to the above segment
    DirNBorSB: NB or SB directory for getting full path
    PatternFNm: Pattern to Search in the sheet name. Default searches for NB I-83 pattern
    Debug: True, Open Source Excel File
    '''
    PathTMS = os.path.join(os.path.expanduser('~'),'OneDrive - Kittelson & Associates, Inc',
                         'Documents', '23746 - I-83 TSMO','TMS-VolProfile')
    Pathfile = os.path.join(PathTMS,DirNBorSB,fileName)
    #Get the excel file in memory. Helps with iterating over sheet names
    x1 = pd.ExcelFile(Pathfile)
    sheet_names = x1.sheet_names
    #Look for the following pattern. Matches would have multiple sheet which need to be aggregated
    pattern = re.compile(PatternFNm)
    # Get the sheets with Lane by lane volume
    sheets = [sheet for sheet in sheet_names if pattern.search(sheet)]
    DatLanes = {} #Data by lane. Not using it . Can it used for debugging
    VolDat = pd.DataFrame() 
    if len(sheets)!=0:
        for sheet in sheets:
            LnNo= re.findall(r'\b\d+\b',sheet)[0]  #Find the lane # from sheet name
            Tpdat = x1.parse(sheet)
            Tpdat = Tpdat[['Hour','Volume']]  # Keep relevant columns
            Tpdat.set_index('Hour',inplace=True) 
            DatLanes['Lane {}'.format(LnNo)]= Tpdat
            if VolDat.shape[0] == 0:
                VolDat = Tpdat #Assign during 1st iteration
            else: 
                VolDat = VolDat+Tpdat # Addition after 1st interation     
    else: 
            VolDat= x1.parse('Volume Totals')
            VolDat = VolDat[['Hour','Volume']]
            VolDat.set_index('Hour',inplace=True)
    if Debug:
        subprocess.Popen([Pathfile],shell=True)  
    VolDat.Volume = (ADT * VolDat.Volume/ sum(VolDat.Volume)).round(2)
    VolDat= VolDat.rename(columns={'Volume':SegNm})       
    return VolDat

#Get Volume Profile for NB
########################################################################################################################
    
NBDat = pd.DataFrame()
for indx,row in NB_KeyVal.iterrows():
    if indx ==0:
        NBDat=GetVolData(row['Segment'],row['TMS_File_Name'],row['ADT'],Debug=False)
    else:
        TempDat = GetVolData(row['Segment'],row['TMS_File_Name'],row['ADT'],Debug=False)
        NBDat = pd.merge(NBDat,TempDat,left_index=True,right_index=True,how='inner')
   
#Check if hourly volumes add up
NBDat.sum(axis=0)     
#NBDat.index = pd.to_datetime(NBDat.index).strftime('%H:%M')
NBDat.index = pd.to_datetime(NBDat.index)
DummyIndex = NBDat.index[-1]+pd.Timedelta('1H')
NBDat.loc[DummyIndex,:]=-99999
#Create additional intervals at 15 mins
NBDat2 = NBDat.resample('15T',convention='start').ffill()
NBDat2.drop(index=DummyIndex,inplace=True)
NBDat2.index = pd.to_datetime(NBDat2.index).strftime('%H:%M')

#Debug
i = 15
Pathfile = os.path.join(PathTMS,'NB-TMS',NB_KeyVal.TMS_File_Name[i])
#subprocess.Popen([Pathfile],shell=True)



#Get Volume Profile for SB
########################################################################################################################
#Get the Key value pair for SB TMS files
SB_KeyVal = pd.read_excel(PathKeyValFi, 'SB-NB-KeyValue', skiprows = 23, usecols = 'C:E',nrows=20)
SBDat = pd.DataFrame()
for indx,row in SB_KeyVal.iterrows():
    if indx ==0:
        SBDat=GetVolData(row['Segment'],row['TMS_File_Name'],row['ADT'],DirNBorSB='SB-TMS',
                        PatternFNm= "^(South|West) *Lane *[0-9] *Volume$",Debug=False)
    else:
        TempDat = GetVolData(row['Segment'],row['TMS_File_Name'],row['ADT'],DirNBorSB='SB-TMS',
                        PatternFNm= "^(South|West) *Lane *[0-9] *Volume$",Debug=False)
        SBDat = pd.merge(SBDat,TempDat,left_index=True,right_index=True,how='inner')
   
#Check if hourly volumes add up
SBDat.sum(axis=0)     
#SBDat.index = pd.to_datetime(SBDat.index).strftime('%H:%M')
SBDat.index = pd.to_datetime(SBDat.index)
DummyIndex = SBDat.index[-1]+pd.Timedelta('1H')
SBDat.loc[DummyIndex,:]=-99999

SBDat2 = SBDat.resample('15T',convention='start').ffill()
SBDat2.drop(index=DummyIndex,inplace=True)
SBDat2.index = pd.to_datetime(SBDat2.index).strftime('%H:%M')



NBDat2.to_excel(os.path.join(PathTMS,'NoBuild-NBVolProflie.xlsx'))
SBDat2.to_excel(os.path.join(PathTMS,'NoBuild-SBVolProflie.xlsx'))

OutFiPa = os.path.join(PathTMS,'NoBuild-NBVolProflie.csv')
#subprocess.Popen([OutFiPa],shell=True)  
OutFiPa = os.path.join(PathTMS,'NoBuild-SBVolProflie.csv')
#subprocess.Popen([OutFiPa],shell=True)  


