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
import xlrd
import re


def I495_RampVol(file, writer):
    x1 = pd.ExcelFile(file)
    x1.sheet_names
    # Skip garbage from 1st 10 rows
    Dat = x1.parse("1", skiprows=10, nrows=48)
    Dat.rename(columns={"Unnamed: 0": "Time"}, inplace=True)
    Dat_AM = Dat[[x for x in Dat.columns if ((re.match("A.M.*", x)) or (x == "Time"))]]
    Dat_PM = Dat[[x for x in Dat.columns if ((re.match("P.M.*", x)) or (x == "Time"))]]
    Dat_AM.columns = [
        "Time",
        "Tue",
        "Wed",
        "Thur",
        "Fri",
        "Sat",
        "Sun",
        "Mon",
        "AvgDay",
    ]
    Dat_AM.loc[:, "Hr"] = Dat_AM.Time.str.split(":", expand=True)[0]
    Dat_AM.loc[:, "Min"] = Dat_AM.Time.str.split(":", expand=True)[1]
    Dat_AM.loc[Dat_AM.Hr == "12", "Hr"] = 0
    Dat_AM = (
        Dat_AM.groupby(["Hr", "Min"])[["Fri", "Sat", "Sun", "Mon"]].sum().reset_index()
    )
    Dat_PM.columns = [
        "Time",
        "Tue",
        "Wed",
        "Thur",
        "Fri",
        "Sat",
        "Sun",
        "Mon",
        "AvgDay",
    ]
    Dat_PM.loc[:, "Hr"] = Dat_PM.Time.str.split(":", expand=True)[0]
    Dat_PM.loc[:, "Min"] = Dat_PM.Time.str.split(":", expand=True)[1]
    Dat_PM.loc[Dat_PM.Hr == "12", "Hr"] = 0
    Dat_PM = (
        Dat_PM.groupby(["Hr", "Min"])[["Fri", "Sat", "Sun", "Mon"]].sum().reset_index()
    )
    Dat_PM.loc[:, "Hr"] = Dat_PM.Hr.astype(np.int64) + 12
    Dat_Fin = pd.concat([Dat_AM, Dat_PM])
    Dat_Fin = Dat_Fin[["Hr", "Min", "Fri", "Sat", "Sun", "Mon"]]
    Dat_Fin = Dat_Fin.set_index(["Hr", "Min"])
    ADT = Dat_Fin[["Fri", "Sat", "Sun", "Mon"]].sum(axis=0)
    ADT = pd.DataFrame(ADT).transpose()
    # Get Flow Rate --- We are given volumes
    Dat_Fin[["Fri", "Sat", "Sun", "Mon"]] = Dat_Fin[["Fri", "Sat", "Sun", "Mon"]] * 4
    # Get % of ADT
    #    Dat_Fin[['Fri','Sat','Sun','Mon']] = Dat_Fin[['Fri','Sat','Sun','Mon']].div(Dat_Fin[['Fri','Sat','Sun','Mon']].sum(axis=0),axis =1)*100
    Dat_Fin[["Fri", "Sat", "Sun", "Mon"]].sum(axis=0) / 4
    Ramp = x1.parse("1", skiprows=2, nrows=1, header=None)
    Ramp_Nm = Ramp.iloc[0, 0]
    Dat_Fin.rename(
        columns={
            "Fri": "Friday_{}-I495".format(Ramp_Nm),
            "Sat": "Saturday_{}-I495".format(Ramp_Nm),
            "Sun": "Sunday_{}-I495".format(Ramp_Nm),
            "Mon": "Monday_{}-I495".format(Ramp_Nm),
        },
        inplace=True,
    )
    Dat_Fin = Dat_Fin[
        [
            "Friday_{}-I495".format(Ramp_Nm),
            "Saturday_{}-I495".format(Ramp_Nm),
            "Sunday_{}-I495".format(Ramp_Nm),
            "Monday_{}-I495".format(Ramp_Nm),
        ]
    ]
    ADT.rename(
        columns={
            "Fri": "Friday_{}-I495".format(Ramp_Nm),
            "Sat": "Saturday_{}-I495".format(Ramp_Nm),
            "Sun": "Sunday_{}-I495".format(Ramp_Nm),
            "Mon": "Monday_{}-I495".format(Ramp_Nm),
        },
        inplace=True,
    )
    startrow1 = 0
    ADT.to_excel(writer, Ramp_Nm, startrow=0, startcol=1)
    Dat_Fin.to_excel(writer, Ramp_Nm, startrow=4, index=True)
    worksheet = writer.sheets[Ramp_Nm]
    worksheet.cell(startrow1 + 1, 1, Ramp_Nm)
    startrow1 = worksheet.max_row + 1


os.chdir(
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Volumes\RampVolumes\I-495"
)
file = "175766 (17) Volume (Week).xls"
files = [
    "175766 (17) Volume (Week).xls",
    "175766 (18) Volume (Week).xls",
    "175766 (19) Volume (Week).xls",
    "175766 (20) Volume (Week).xls",
]
writer1 = pd.ExcelWriter("I495_Volumes.xlsx")
for file in files:
    I495_RampVol(file, writer1)
writer1.save()
