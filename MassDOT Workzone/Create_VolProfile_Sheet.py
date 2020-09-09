# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 10:22:48 2019

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


# ****************************************************************************
EB_Mainline = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Volumes\Main-Counters\AET07-EB\AET07-EB_VolProfile.xlsx"
x1 = pd.ExcelFile(EB_Mainline)
x1.sheet_names
Dat_EB_MainLine = x1.parse("VolProfile")
Dat_EB_MainLine = Dat_EB_MainLine.drop(columns=["Unnamed: 0", "Unnamed: 1"])
# https://stackoverflow.com/questions/46250972/split-columns-into-multiindex-with-missing-columns-in-pandas
# Multiindex from column names
idx = Dat_EB_MainLine.columns.str.split("_", expand=True)
Dat_EB_MainLine.columns = idx

# ****************************************************************************
I_495 = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Volumes\RampVolumes\I-495\I495_Volumes.xlsx"
x1 = pd.ExcelFile(I_495)
x1.sheet_names
I_495_Dat_EB_Off = x1.parse("I-90 EB Offramp")
I_495_Dat_EB_Off = I_495_Dat_EB_Off.drop(
    columns=[I_495_Dat_EB_Off.columns[0], I_495_Dat_EB_Off.columns[1]]
)
idx = I_495_Dat_EB_Off.columns.str.split("_", expand=True)
I_495_Dat_EB_Off.columns = idx
# ****************************************************************************
I_495_Dat_EB_On = x1.parse("I-90 EB Onramp")
I_495_Dat_EB_On = I_495_Dat_EB_On.drop(
    columns=[I_495_Dat_EB_On.columns[0], I_495_Dat_EB_On.columns[1]]
)
idx = I_495_Dat_EB_On.columns.str.split("_", expand=True)
I_495_Dat_EB_On.columns = idx
# ****************************************************************************
EB_Dat = pd.merge(Dat_EB_MainLine, I_495_Dat_EB_Off, left_index=True, right_index=True)
EB_Dat = pd.merge(EB_Dat, I_495_Dat_EB_On, left_index=True, right_index=True)
mux = pd.MultiIndex.from_product(
    [
        ["Friday", "Saturday", "Sunday", "Monday"],
        [
            "AET07-EB",
            "I-90 EB Offramp-I495",
            "I-90 EB Onramp-I495",
            "EB Route 9 Off-Ramp",
            "EB Route 9 On-Ramp",
            "VSep",
        ],
    ],
    names=["Day", "Seg"],
)
EB_Dat = EB_Dat.reindex(mux, axis=1)

idx = pd.IndexSlice

# R11527: RAMP-RT 90 EB TO RT 9
EB_Dat.loc[0, idx[:, ["EB Route 9 Off-Ramp"]]] = 5600
# R11528: 	RAMP-RT 9 TO RT 90 EB
EB_Dat.loc[0, idx[:, ["EB Route 9 On-Ramp"]]] = 11645


EB_Dat.loc[
    1,
    idx[
        :,
        [
            "AET07-EB",
            "I-90 EB Offramp-I495",
            "I-90 EB Onramp-I495",
            "EB Route 9 Off-Ramp",
            "EB Route 9 On-Ramp",
            "VSep",
        ],
    ],
] = [1, 3, 5, 14, 16, -999] * 4
EB_Dat.rename(index={1: "Freeval Seg"}, inplace=True)

# ****************************************************************************
# WB
# ****************************************************************************
WB_Mainline = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Volumes\Main-Counters\AET09-WB\AET09-WB_VolProfile.xlsx"
x1 = pd.ExcelFile(WB_Mainline)
x1.sheet_names
Dat_WB_MainLine = x1.parse("VolProfile")
Dat_WB_MainLine = Dat_WB_MainLine.drop(columns=["Unnamed: 0", "Unnamed: 1"])
# https://stackoverflow.com/questions/46250972/split-columns-into-multiindex-with-missing-columns-in-pandas
# Multiindex from column names
idx = Dat_WB_MainLine.columns.str.split("_", expand=True)
Dat_WB_MainLine.columns = idx

# ****************************************************************************
x1 = pd.ExcelFile(I_495)
x1.sheet_names
I_495_Dat_WB_Off = x1.parse("I-90 WB Offramp")
I_495_Dat_WB_Off = I_495_Dat_WB_Off.drop(
    columns=[I_495_Dat_WB_Off.columns[0], I_495_Dat_WB_Off.columns[1]]
)
idx = I_495_Dat_WB_Off.columns.str.split("_", expand=True)
I_495_Dat_WB_Off.columns = idx
# ****************************************************************************
I_495_Dat_WB_On = x1.parse("I-90 WB Onramp")
I_495_Dat_WB_On = I_495_Dat_WB_On.drop(
    columns=[I_495_Dat_WB_On.columns[0], I_495_Dat_WB_On.columns[1]]
)
idx = I_495_Dat_WB_On.columns.str.split("_", expand=True)
I_495_Dat_WB_On.columns = idx
# ****************************************************************************
WB_Dat = pd.merge(Dat_WB_MainLine, I_495_Dat_WB_Off, left_index=True, right_index=True)
WB_Dat = pd.merge(WB_Dat, I_495_Dat_WB_On, left_index=True, right_index=True)
mux = pd.MultiIndex.from_product(
    [
        ["Friday", "Saturday", "Sunday", "Monday"],
        [
            "AET09-WB",
            "WB Route 9 Off-Ramp",
            "WB Route 9 On-Ramp",
            "I-90 WB Offramp-I495",
            "I-90 WB Onramp-I495",
            "VSep",
        ],
    ],
    names=["Day", "Seg"],
)
WB_Dat = WB_Dat.reindex(mux, axis=1)

idx = pd.IndexSlice

# R11544: 	RAMP-RT 90 WB TO RT 9
WB_Dat.loc[0, idx[:, ["WB Route 9 Off-Ramp"]]] = 13713
# R11545: 	RAMP-RT 9 TO RT 90 WB
WB_Dat.loc[0, idx[:, ["WB Route 9 On-Ramp"]]] = 5533

WB_Dat.loc[
    1,
    idx[
        :,
        [
            "AET09-WB",
            "WB Route 9 Off-Ramp",
            "WB Route 9 On-Ramp",
            "I-90 WB Offramp-I495",
            "I-90 WB Onramp-I495",
            "VSep",
        ],
    ],
] = [1, 5, 7, 16, 18, -999] * 4
WB_Dat.rename(index={1: "Freeval Seg"}, inplace=True)
OutFi = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Volumes\Volumes-Processed.xlsx"
writer = pd.ExcelWriter(OutFi)
EB_Dat.to_excel(writer, "EB Data")
WB_Dat.to_excel(writer, "WB Data")
writer.save()

subprocess.Popen([OutFi], shell=True)
