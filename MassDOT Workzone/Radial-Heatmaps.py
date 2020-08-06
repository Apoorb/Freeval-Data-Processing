# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 16:44:14 2019

@author: abibeka
"""

import pandas as pd
import os 
import matplotlib.pyplot as plt
import numpy as np
import calendar

# https://stackoverflow.com/questions/40352607/time-wheel-in-python3-pandas
#https://stackoverflow.com/questions/36513312/polar-heatmaps-in-python

#os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Freeval\Base-Models')
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\MassDOT\Freeval\1-Bridge-Closed-Per-Weekend')

#x1 = pd.ExcelFile('Freeval-TT.xlsx')
#x1.sheet_names
#EB = x1.parse('EB')
#
#WB = x1.parse('WB')

x1 = pd.ExcelFile('1-Bridge-Freeval-TT.xlsx')
EB = x1.parse('EB-Woodland')
WB = x1.parse('WB-Flanders')

# generate the table with timestamps
#np.random.seed(1)
#times = pd.Series(pd.to_datetime("Oct 1 '18 at 0:00") + 
#                  pd.to_timedelta(np.random.rand(10000)*60*24*40, unit='m'))
## generate counts of each (weekday, hour)
#data = pd.crosstab(times.dt.weekday, 
#                   times.dt.hour.apply(lambda x: '{:02d}:00'.format(x))).fillna(0)
#data.index = [calendar.day_name[i][0:3] for i in data.index]
#data = data.T

#Date = pd.Series(pd.to_datetime("Oct 1 '18 at 0:00") + 
#                pd.to_timedelta(np.arange(0,96)*15, unit='m'))
#Time = ['{}:{}'.format(a,b) for a,b in zip(Date.dt.hour, Date.dt.minute)]
#
#dat.loc[:,'Hour'] = Date.dt.hour
EB = EB.groupby('Hour')[['Fri','Sat','Sun','Mon']].mean()/ 60
WB = WB.groupby('Hour')[['Fri','Sat','Sun','Mon']].mean() / 60 # change to min
EB.name ='EB'
WB.name ='WB'

# produce polar plot
#https://stackoverflow.com/questions/22416965/adding-a-colorbar-to-a-polar-contourf-multiplot
fig, axs = plt.subplots(1,2, figsize =(12,6), subplot_kw=dict(projection='polar'))
for i,data in zip([0,1],[EB, WB]):
    axs[i].set_theta_zero_location("N")
    axs[i].set_theta_direction(-1)
    
    # plot data
    #https://www.geeksforgeeks.org/numpy-meshgrid-function/
    theta, r = np.meshgrid(np.linspace(0,2*np.pi,len(data)+1),np.arange(len(data.columns)+1))
    pc = axs[i].pcolormesh(theta,r,data.T.values, cmap="Reds")
    # set ticklabels
    pos,step = np.linspace(0,2*np.pi,len(data),endpoint=False, retstep=True)
    pos += step/2.
    axs[i].set_xticks(pos)
    axs[i].set_xticklabels(data.index)
    axs[i].set_yticks(np.arange(len(data.columns)))
    axs[i].set_yticklabels(data.columns)
    axs[i].set_title(data.name,y=1.08)
#https://stackoverflow.com/questions/22416965/adding-a-colorbar-to-a-polar-contourf-multiplot
# Fixing the position issue
ax3 = fig.add_axes([0.9, 0.1, 0.03, 0.8])
fig.colorbar(pc, ax3)
fig.subplots_adjust(left=0.05,right=0.85)
plt.show()
fig.savefig('Radial')





#*******************************************************************************************************
# Plot the diff days on diff subplots
def PlotRadials (data = WB, name = "WB"):
    #
    # Plot the diff days on diff subplots
    fig_WB, axs_WB = plt.subplots(2,2,figsize=(4.5,4) ,subplot_kw=dict(projection='polar'))
    pc_s = []
    for ax,i in zip(axs_WB.flatten(),[0,1,2,3]):
        data1 = pd.DataFrame(data.iloc[:,i])
        theta, r = np.meshgrid(np.linspace(0,2*np.pi,len(data1)+1),np.arange(len(data1.columns)+1))
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        # plot data
        #https://www.geeksforgeeks.org/numpy-meshgrid-function/
        pc = ax.pcolormesh(theta,r,data1.T.values, cmap="Reds",vmin = 5, vmax= 20)
        pc_s.append(pc)
        # set ticklabels
        pos,step = np.linspace(0,2*np.pi,len(data1),endpoint=False, retstep=True)
        pos += step/2.
        ax.set_xticks(pos)
        ax.set_xticklabels(data1.index)
        ax.set_yticks(np.arange(len(data1.columns)))
        ax.set_yticklabels(data1.columns)
    #https://stackoverflow.com/questions/22416965/adding-a-colorbar-to-a-polar-contourf-multiplot
    # Fixing the position issue
    
    fig_WB.subplots_adjust(hspace=0.5, wspace=0.5)
    ax3 = fig_WB.add_axes([0.9, 0.1, 0.03, 0.8])
    #
    #ax4 = fig_WB.add_axes([1.1, 0.1, 0.03, 0.8])
    #ax5 = fig_WB.add_axes([1.2, 0.1, 0.03, 0.8])
    #ax6 = fig_WB.add_axes([1.4, 0.1, 0.03, 0.8])
    
    fig_WB.colorbar(pc_s[0], ax3)
    #fig_WB.colorbar(pc_s[1], ax4)
    #fig_WB.colorbar(pc_s[2], ax5)
    #fig_WB.colorbar(pc_s[3], ax6)
    fig_WB.subplots_adjust(left=0.05,right=0.85)
    fig_WB.suptitle("{} Travel Time (min)".format(name))
    plt.subplots_adjust(top=0.85)
    plt.show()
    fig_WB.savefig('{}.jpg'.format(name))
    return(0)
    
    
    
PlotRadials(EB, name ='EB')

PlotRadials(WB)




