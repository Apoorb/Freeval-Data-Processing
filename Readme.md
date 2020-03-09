# I-83 TSMO: 
- GetFreevalInputVolProfile.py: For I-83 TSMO project. Reads TMS station profiles and apply it to ADT
- Plot-TravelTime.py: Plot TT for SB

# MassDOT Workzone: 
- VolumeProfile_MassDOT.py: Get the 24 volume profile in % for mainline
- VolumeProfile_MassDOT---Get_15_min_vol.py : Get the mainline 15 min flow rate 
- ProcessRampData-MassDOT.py: Get the Ramp volume profile for I-495
- Create_VolProfile_Sheet.py : Get 96 15 min interval flow rate for mainline and ramps 
- Radial-Heatmaps.py: Create Radial heatmaps for congestion
- Process_Npmrds_Data.py: Process NPMRDS data

# Feeval-PA Scripts:
- Freeval_PA_Dat_Process_CleanVersion.py: Script to get LRS 
- Clean_Group_FreevalPA_SegmentationData.py: Groupby and clean Freeval-PA data
- CommonFunctions_FreevalPA_Cleaning.py: Dump all functions here
- Freeval_PA_Dat_Process_CleanVersion.py: Messy script. 1st attempt for cleaning data. 

# NCHRP 7-26
- Process-Freeval-PA-Segmentation-NCHRP7-26.py: Script for getting ramp types from PennDOT segmentation database
		- High false negatives for Lane add, lane drop, major merge and diverge types	
- Process-Freeval-NC-Segmentation-NCHRP7-26.py: Copy of above script for processing NC segmentation database.
		- High false negatives for Lane add, lane drop, major merge and diverge types