import geopandas as gpd
import inflection


def read_data(filename_gdf, layer_gdf, sort_fkey_asc=True):
    gdf = gpd.read_file(filename=filename_gdf, layer=layer_gdf)
    gdf.columns = [inflection.underscore(colname) for colname in gdf.columns]
    subset_gdf = gdf[['name', 'juris', 'cty_code', 'st_rt_no', 'seg_bgn', 
                      'seg_end', 'seg_no', 'side_ind', 'seg_lngth_', 
                      'seg_lngth_2','seq_no', 'fkey', 'fseg', 'flength', 
                      'fdir', 'foffset', 'terrain_ty', 'fgrade', 'c_offset_b',
                      'cum_offset', 'c_offset_1', 'cum_offs_1', 'nlf_cntl_b', 
                      'nlf_cntl_e', 'dir_ind', 'fac_type', 'geometry']]
    subset_gdf.sort_values(by = ['name', 'fkey'], inplace = True,ascending = \
                           ['True','True'])
    return subset_gdf





def read_subset_dat(path):
    ""

    