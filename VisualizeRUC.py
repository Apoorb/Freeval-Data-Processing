# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 10:28:54 2020

@author: abibeka
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import plotly.express as px

Path = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT"

file = "User Cost Calculation Example.xlsx"

FilePath = os.path.join(Path,file)

x1 = pd.ExcelFile(FilePath)
x1.sheet_names
dat = x1.parse("RawData")

dat.columns
sns.catplot(x="Day",y="Road User Cost", hue = "Case",col="Bridge",col_wrap=2,kind="bar",data=dat)

