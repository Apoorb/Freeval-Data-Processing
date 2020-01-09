#File Name:  VisulizeRUC.R
#Created by: Apoorba Bibeka
#Creation date: Jan 8 2019
#Date Modified : 
#Purpose:To plot RUC data from MassDOT I-90
#Last executed:
Sys.time()

#1 Housekeeping
#********************************************************************************************************************
ls()
rm(list=ls())
ls()

#2 Load Libraries
#********************************************************************************************************************
library(readxl)
library(ggplot2)
library(data.table)
library(writexl)
library(ggrepel)
#3 Read Data
#********************************************************************************************************************
dir1="C:/Users/abibeka/OneDrive - Kittelson & Associates, Inc/Documents/MassDOT"
setwd(dir1)
dat<- read_excel("User Cost Calculation Example.xlsx",sheet="RawData")
dat<-data.table(dat)
str(dat)
dodgewidth <- position_dodge(width=1.5)
g1 = ggplot(dat, aes(x=Day,y=`Road User Cost`, colour = factor(Case)))+geom_bar(stat="identity",position=dodgewidth,fill="white") + facet_wrap("Bridge", nrow=2)
g1 = g1+theme_minimal()+scale_color_brewer(palette="Dark2")
g1 = g1+ geom_text_repel(aes(x=Day, label=`Road User Cost`),nudge_y = 0.05, direction="x",angle =0, hjust=0.5,vjust=-0.5)
g1







