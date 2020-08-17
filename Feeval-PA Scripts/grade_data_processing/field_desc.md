Fields and description

RMS SEG Data:  https://data-pennshare.opendata.arcgis.com/datasets/rmsseg-state-roads-1

RMS Admin Data = https://data-pennshare.opendata.arcgis.com/datasets/rmsadmin-administrative-classifications-of-roadway

RMS Traffic Data = https://data-pennshare.opendata.arcgis.com/datasets/rmstraffic-traffic-volumes

H:\21\21659 - E03983 - PennDOT BOMO Safety\WO4 - FreeVal\gis\data\Custom Tools\Intersected_Tables_XLS
OBJECTID          int64: Not useful, ignore this field
					- Network linear feature identifier for use in dynamic segmentation. NLF_ID is a unique control number internally assigned to represent a single contiguous section of a route within a county
NLF_ID            int64: Not useful, ignore this field
					- Network linear feature identifier for use in dynamic segmentation. NLF_ID is a unique control number internally assigned to represent a single contiguous section of a route within a county
NLF_CNTL_B      float64: Not useful, ignore this field
					- Distance from start of NLF to segment begin point
NLF_CNTL_E      float64: Not useful, ignore this field
					- Distance from start of NLF to segment end point
**Name              int64: 
					- Contains 
						- Route #, 
						- jusidiction and 
						- Freeval segment No**
FolderPath       object: Can use to verify
**ST_RT_NO          int64: Name has it---State route number**
**CTY_CODE          int64: PennDOT`s County code NOT the Commonwealth`s**
**DISTRICT_N        int64: PennDOT Engineering District number**
**JURIS             int64: Name has it---Jurisdiction or road ownership**
SEG_NO            int64: Not useful, ignore this field
SEG_LNGTH_        int64: Not useful, ignore this field
SEQ_NO            int64: Not useful, ignore this field
**DIR_IND          object: Directional indicator:
					- B, E, N, O, S and W**
**FAC_TYPE          int64: One-way indicator--- 1: One-way and 2: Two-way**
**TOTAL_WIDT        int64: 	
					- Total paved width measurement from edge of pavement to edge of pavement, including any parking lanes**
**LANE_CNT          int64: Number of lanes on a segment. Does not include turning lanes, passing lanes**
DIVSR_TYPE        int64: Type of barrier or median on divided road segments 
DIVSR_WIDT        int64: Width of divisor in feet
**CUR_AADT          int64: Current annual average daily traffic**
ACCESS_CTR        int64: Access control code
STREET_NAM       object: Not useful, ignore this field
**TRAF_RT_NO       object: Keep it : Traffic route prefix, such as US and PA --- should have been the other way round**
**TRAF_RT__1        int64: Keep it : Traffic route number** 
**URBAN_RURA        int64: Urban/rural code : 4 categories**
SIDE_IND          int64: Potential duplicate: Right / Left side indicator
CUM_OFFSET        int64: Not useful, ignore this field
CUM_OFFS_1        int64: Not useful, ignore this field
KEY_UPDATE        int64: Not useful, ignore this field
ATTR_UPDAT        int64: Not useful, ignore this field
ROUTE_DIR        object: Duplicate of DIR_IND
					- B, E, N, O, S and W
					- looks messy
Shape_Length    float64: Might be useful --- I think Alexandra created this field
ST_RT_NO_int      int64: Incomplete, ignore
RMSTRAFFIC       object: Not useful, ignore this field
ST_RT_NO2         int64: Not useful, ignore this field
CTY_CODE2         int64: Not useful, ignore this field
DISTRICT_N2       int64: Not useful, ignore this field
JURIS2            int64: Not useful, ignore this field
SEG_BGN           int64: Not useful, ignore this field
OFFSET_BGN        int64: Not useful, ignore this field
SEG_END           int64: Not useful, ignore this field
OFFSET_END        int64: Not useful, ignore this field
SEG_PT_BGN        int64: Not useful, ignore this field
SEG_PT_END        int64: Not useful, ignore this field
SEG_LNGTH_2       int64: Not useful, ignore this field
TRK_PCT           int64: Truck percent. Excludes pick-ups, panels, and light trucks 
K_FACTOR          int64: Ratio of Design Hour Values (DHV) to Annual Average Daily Traffic (AADT) 
D_FACTOR          int64: 
					- Directional traffic split. For the highest hour of traffic, the percentage of the highest direction of the highest hour total, displayed in increments of 5.
T_FACTOR          int64: 
					- Truck factor. Calculated by dividing the number of trucks in the highest hour by the number of vehicles in the highest hour and multiplying by 100.
DIR_IND2         object: duplicate, missing and Not needed
SIDE_IND2         int64: duplicate, missing and Not needed
TERRAIN_TY      float64: Missing
GRADES_A        float64: Not useful, ignore this field
GRADES_B        float64: Not useful, ignore this field
GRADES_C        float64: Not useful, ignore this field
GRADES_D        float64: Not useful, ignore this field
GRADES_E          int64: Not useful, ignore this field
GRADES_F          int64: Not useful, ignore this field