# -*- coding: utf-8 -*-
"""
Created on Thu Sep 26 08:13:49 2019

@author: abibeka
"""

# 0.0 Housekeeping. Clear variable space
from IPython import get_ipython  # run magic commands

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


def read_vol(file):
    """
    Read Volume data
    Make it narrow
    Remove Thanksgiving data
    Get day of the week
    """
    WrkBk = pd.ExcelFile(file)
    WrkBk.sheet_names
    # Get the Month and year from sheet name
    Month, Year = WrkBk.sheet_names[0].split("_")[1:]
    Dat = WrkBk.parse(WrkBk.sheet_names[0], skiprows=9)
    Dat.columns
    Dat.rename(columns={"Unnamed: 0": "Day"}, inplace=True)
    Dat.loc[:, "Month"] = Month
    Dat.loc[:, "Year"] = Year
    Dat.loc[:, "Date"] = pd.to_datetime(Dat[["Day", "Month", "Year"]])

    # Drop unused data
    Dat1 = Dat
    Dat1 = Dat1.drop(columns=["Day", "Month", "Year", "TOTAL"]).set_index("Date")
    # ADT data
    Dat_adt = Dat[["Date", "TOTAL"]]
    Dat_adt = Dat_adt.set_index("Date")
    # Wide to Narrow datat
    Dat1 = Dat1.stack().reset_index()
    Dat1.columns
    # Combine Date and time
    Dat1.Date = Dat1.apply(lambda r: pd.datetime.combine(r["Date"], r["level_1"]), 1)
    Dat1 = Dat1.set_index("Date").drop("level_1", axis=1)
    Dat1.rename(columns={0: "Vol"}, inplace=True)
    Dat1.index = Dat1.index.map(lambda x: x.replace(second=0))

    # Get the Day Name
    Dat1.loc[:, "Day"] = Dat1.index.day_name()
    mask = Dat1.Day.isin(["Friday", "Saturday", "Sunday", "Monday"])
    Dat1 = Dat1[mask]

    Dat_adt.loc[:, "Day"] = Dat_adt.index.day_name()
    mask2 = Dat_adt.Day.isin(["Friday", "Saturday", "Sunday", "Monday"])
    Dat_adt = Dat_adt[mask2]
    # Remove Thanksgiving : Nov 22 2018 (Fri) to Nov 26 2018 (Mon)
    Dat1 = Dat1[
        ~(
            (Dat1.index >= pd.datetime(2018, 11, 22))
            & (Dat1.index < pd.datetime(2018, 11, 27))
        )
    ]

    Dat_adt = Dat_adt[
        ~(
            (Dat_adt.index >= pd.datetime(2018, 11, 22))
            & (Dat_adt.index < pd.datetime(2018, 11, 27))
        )
    ]

    return (Dat1, Dat_adt)


# Dat_Long = Dat
def Process_Dat(Dat_Long, Dat_Adt):
    """
    Dat_Long = Hourly data
    Dat_Adt = Adt Data for the day 
    """
    # Get the Sum of Volume by the Hour
    # Dat2 = Dat1.groupby(['Day',Dat1.index.hour]).agg({'Vol':{'Sum':np.sum,"Count": 'count'}})
    Dat2 = Dat_Long.groupby(["Day", Dat_Long.index.hour])["Vol"].mean().round()
    Dat2 = Dat2.swaplevel()
    Dat2 = Dat2.unstack()
    Dat2 = Dat2[["Friday", "Saturday", "Sunday", "Monday"]]
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.div.html
    # https://stackoverflow.com/questions/26537878/pandas-sum-across-columns-and-divide-each-cell-from-that-value
    # Check
    Dat3 = Dat2
    Dat3 = Dat3.reset_index()
    Dummy_index = pd.MultiIndex.from_product(
        [np.arange(0, 24), np.arange(0, 60, 15)], names=["Hr", "Min"]
    )
    Dummy_Dat = pd.DataFrame(index=Dummy_index).reset_index()
    Dat3 = Dummy_Dat.merge(Dat3, left_on="Hr", right_on="Date", how="inner")
    Dat3.drop(columns="Date", inplace=True)
    Dat3.sum(axis=0)
    Dat3.columns = [x + "_{}".format(Counter) for x in Dat3.columns]
    # ADT values
    Dat_ADT = Dat_Adt.groupby(["Day"])["TOTAL"].mean().astype("int")
    Dat_ADT = pd.DataFrame(Dat_ADT)
    Dat_ADT = Dat_ADT.transpose()
    Dat_ADT = Dat_ADT[["Friday", "Saturday", "Sunday", "Monday"]]
    Dat_ADT.columns = [x + "_{}".format(Counter) for x in Dat_ADT.columns]

    return (Dat3, Dat_ADT)


# Change File/folder name/location here
##########################################################################################################
Counter = "AET07-EB"
# Counter = 'AET09-WB'
# Counter = 'AET08-WB'
# Counter = 'AET08-EB'

# Counters =['AET07-WB','AET09-EB','AET08-WB','AET08-EB','AET07-EB','AET09-WB']
Counters = ["AET07-EB", "AET09-WB"]
for Counter in Counters:
    os.chdir(
        r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Volumes\Main-Counters\{}".format(
            Counter
        )
    )

    file_Nov = "MonthlyVolumeReport_11_2018.xlsx"
    file_Dec = "MonthlyVolumeReport_10_2018.xlsx"
    file = file_Nov
    Dat = pd.concat([read_vol(file_Nov)[0], read_vol(file_Dec)[0]])
    Dat_Adt = pd.concat([read_vol(file_Nov)[1], read_vol(file_Dec)[1]])

    VolProfile, ADT = Process_Dat(Dat, Dat_Adt)

    Dat_Long = Dat
    #    VolProfile1 =VolProfile
    #    VolProfile1 = VolProfile1.multiply(ADT.values,axis=1)
    #    VolProfile1.sum(axis=0)
    #    VolProfile1 = VolProfile1.stack().reset_index().rename(columns = {0:'PerVol'})
    #    VolProfile1.rename(columns={'PerVol':'Flow Rate (Veh/Hr)','Date':'Hour of Day'},inplace=True)
    #    VolProfile1
    #    fig,ax1 = plt.subplots(figsize= (10,6))
    #    g = sns.catplot(data = VolProfile1, x = 'Hour of Day', y = 'Flow Rate (Veh/Hr)', hue = 'Day',kind='point',ax=ax1)
    #    ax1.set_ylim(0,6000)
    #    ax1.set_title('Counter: {}'.format(Counter))
    #    ax1.axhline(3400, ls='--',color='orange')
    #    ax1.axhline(4000, ls='--',color='red')
    #    fig.savefig('{}_VolProfile.pdf'.format(Counter))

    # L1= np.arange(0,24)
    # L2 =  [0,15,30, 45]
    # DummyDat = pd.DataFrame(list(product(L1, L2)), columns=['Hr', 'Min'])
    # VolProfile_15Min = DummyDat.merge(VolProfile,left_on ='Hr',right_on= 'Date')

    startrow1 = 0
    writer = pd.ExcelWriter("{}_VolProfile.xlsx".format(Counter))
    # VolProfile_15Min.to_excel(writer,'VolProfile_15Min',startrow = startrow1+2,index=False)
    # worksheet = writer.sheets['VolProfile_15Min']
    # startrow1 = worksheet.max_row +1
    # ADT.to_excel(writer,'VolProfile_15Min',startrow = startrow1,startcol=1)
    ADT.to_excel(writer, "VolProfile", startrow=0, startcol=1)
    VolProfile.to_excel(writer, "VolProfile", startrow=4, index=False)
    writer.save()


# subprocess.Popen(['{}_VolProfile.xlsx'.format(Counter)],shell=True)
