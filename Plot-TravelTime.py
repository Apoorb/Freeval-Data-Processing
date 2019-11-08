# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:05:15 2019

@author: abibeka
"""

# 0.0 Housekeeping. Clear variable space
#******************************************************************************************
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

import matplotlib.pyplot as plt
import os
import numpy as np
import pandas as pd
import matplotlib.style
import matplotlib as mpl
import seaborn as sns

os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\23746 - I-83 TSMO\FreeVal Models---SB')

x1  = pd.ExcelFile('Final-Results.xlsx')
x1.sheet_names

dat1 = x1.parse('Sheet1')

mpl.rcParams['grid.color'] = 'k'
mpl.rcParams['grid.linestyle'] = ':'
mpl.rcParams['grid.linewidth'] = 0.5


c1  = sns.color_palette("Set2")
sns.set_style("white")
fig, ax = plt.subplots(1,2,sharey =True)
ax[0].plot(dat1.iloc[:,0],dat1.iloc[:,1])
ax[0].plot(dat1.iloc[:,0],dat1.iloc[:,2])
ax[1].plot(dat1.iloc[:,0],dat1.iloc[:,3])
ax[1].plot(dat1.iloc[:,0],dat1.iloc[:,4])
ax[1].plot(dat1.iloc[:,0],dat1.iloc[:,5])
ax[1].plot(dat1.iloc[:,0],dat1.iloc[:,6])
ax[1].plot(dat1.iloc[:,0],dat1.iloc[:,7])
