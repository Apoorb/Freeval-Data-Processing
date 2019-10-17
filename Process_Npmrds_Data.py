# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 13:30:31 2019

@author: abibeka
"""
#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()


import pandas as pd
import os
import glob
import numpy as np
import seaborn as sns
import subprocess
from itertools import product
import matplotlib.pyplot as plt
import xlrd
import re 

EB_Routes = {
'129+04678' : "Wood St. to 'Exit Ramp to I-495 on EB I-90'",
'129P04678' : "'Exit Ramp to I-495 on EB I-90' to 'I-495 Entrance Ramp on EB 1-90'",
'129+04423' : "'I-495 Entrance Ramp on EB 1-90' to 'Exit Ramp to Route 9 on EB I-90'", 
'129P04423' : "'Exit Ramp to Route 9 on EB I-90' to 'Route 9 Entrance Ramp on EB 1-90'",
'129+04424' : "'Route 9 Entrance Ramp on EB 1-90' to Edgell St."}

WB_Routes = {
'129-04423' : "Edgell St. to 'Exit Ramp to Route 9 on WB I-90'",
'129N04423' : "'Exit Ramp to Route 9 on WB I-90' to 'Route 9 Entrance Ramp on WB 1-90'",
'129-04678' : "'Route 9 Entrance Ramp on WB 1-90' to 'Exit Ramp to I-495 on WB I-90,",
'129N04678' : "'Exit Ramp to I-495 on WB I-90' to 'I-495 Entrance Ramp on WB 1-90'",
'129-04677' : "'I-495 Entrance Ramp on WB 1-90' to Wood St."}
All_Routes_Nm = EB_Routes.copy()
All_Routes_Nm.update(WB_Routes)
        
    
All_Routes_No = {
'129+04678' :1,
'129P04678' :2,
'129+04423' :3, 
'129P04423' :4,
'129+04424' :5,
'129-04423' : 1,
'129N04423' : 2,
'129-04678' : 3,
'129N04678' : 4,
'129-04677' : 5}

os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Speeds')

#Read keys
KeyVal= pd.read_csv('TMC_Identification.csv',usecols =['tmc','direction','miles'])
KeyVal.loc[:,'Order'] = KeyVal.tmc.apply(lambda x: All_Routes_No[x])
KeyVal.loc[:,'Name'] = KeyVal.tmc.apply(lambda x: All_Routes_Nm[x])
# Read Values 
date_col = ['measurement_tstamp']
Dat = pd.read_csv('NovOctTravelTimes_2018.csv',parse_dates = date_col)
Dat.loc[:,'Day'] = Dat.measurement_tstamp.dt.day_name()
Dat.loc[:,'Hr'] =Dat.measurement_tstamp.dt.hour
Dat.loc[:,'Min'] =Dat.measurement_tstamp.dt.minute
# Remove Thanksgiving : Nov 22 2018 (Fri) to Nov 26 2018 (Mon)
Data_del_len =Dat.shape[0]-Dat[~((Dat.measurement_tstamp >= pd.datetime(2018,11,22)) & (Dat.measurement_tstamp < pd.datetime(2018,11,27)))].shape[0]
Data_del_len/96/10 #96 time intervals and 10 segments--- should get roughly 4 days
Dat = Dat[~((Dat.measurement_tstamp >= pd.datetime(2018,11,22)) & (Dat.measurement_tstamp < pd.datetime(2018,11,27)))]
#Dat.shape[0]/96/10
Dat.columns
Dat2 = Dat.groupby(['tmc_code','Day','Hr','Min'])['travel_time_minutes'].mean()
Dat2 = Dat2.swaplevel(1,3)
Dat2 = Dat2.swaplevel(1,2)
Dat2 = Dat2.unstack()
Dat2 = Dat2[['Friday','Saturday','Sunday','Monday']]
Dat2 = Dat2.reset_index()
Dat2 = Dat2.merge(KeyVal,left_on='tmc_code',right_on='tmc')
#Dat.dtypes
# Get Speed Data
Dat3 = Dat2.copy()
#Get TT Data also 
Dat4 = Dat2.copy()
Dat4 = Dat4.groupby(['direction','Hr','Min'])['Friday','Saturday','Sunday','Monday'].sum().reset_index()
#Name would be used during pivoting
Dat4.loc[:,'Name'] = 'Total_TT'
Dat4 = Dat4.set_index(['Hr','Min','Name','direction'])
Dat4 = Dat4* 60

#Get Speed data from average TT in minutes and Distance in miles 
Dat3['Friday'] = Dat3['miles']*60/Dat3['Friday']
Dat3['Saturday'] = Dat3['miles']*60/Dat3['Saturday']
Dat3['Sunday'] = Dat3['miles']*60/Dat3['Sunday']
Dat3['Monday'] = Dat3['miles']*60/Dat3['Monday']
Dat3 = Dat3.set_index(['Hr','Min','Name','direction'])
Dat3 = pd.concat([Dat3,Dat4])
Dat3 =Dat3.drop(columns = 'tmc_code')
Dat3 = Dat3.unstack().unstack()
Dat3 = Dat3.swaplevel(0,1,axis=1)
mux = pd.MultiIndex.from_product([['EASTBOUND','WESTBOUND'],['Friday','Saturday','Sunday','Monday'],
                                  list(All_Routes_Nm.values())+['Vsep','Total_TT']], names=['Dir','Day','Seg'])
Dat3 = Dat3.reindex(mux,axis=1)
dir1= 'WESTBOUND'
day = 'Sunday'
idx= pd.IndexSlice
Dat3.loc[:,idx[dir1,day,:]]  = Dat3.loc[:,idx[dir1,day,:]].fillna(method ='ffill')

OutFi= 'Speed_Processed.xlsx'
writer = pd.ExcelWriter(OutFi)
dir1= 'EASTBOUND'
day = 'Friday'
idx= pd.IndexSlice
Dat3.loc[:,idx[:,:,'Vsep']] = "||"
for dir1 in  ['EASTBOUND','WESTBOUND']:
    for day in ['Friday','Saturday','Sunday','Monday']:
        Fin_Dat = Dat3.loc[:,idx[dir1,day,:]]
        Fin_Dat = Fin_Dat.dropna(axis=1)
        Fin_Dat.to_excel(writer,'{}_{}'.format(day,dir1))
        

writer.save()
#
#Dat4 = Dat2
#Dat4[['Friday','Saturday','Sunday','Monday']] = (Dat4[['Friday','Saturday','Sunday','Monday']])/60
#Dat4[['Friday','Saturday','Sunday','Monday']]  =(1/( Dat4[['Friday','Saturday','Sunday','Monday']])).multiply(Dat4.miles,axis=0)