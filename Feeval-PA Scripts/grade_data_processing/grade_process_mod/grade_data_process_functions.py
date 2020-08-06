import geopandas as gpd
import inflection
import os

def read_data(filename_gdf, layer_gdf):
    """
    Parameters
    ----------
    filename_gdf
    layer_gdf

    Returns
    -------
    """
    gdf = gpd.read_file(filename=filename_gdf, layer=layer_gdf)
    gdf.columns = [inflection.underscore(colname) for colname in gdf.columns]
    subset_gdf = gdf[['name', 'juris', 'cty_code', 'st_rt_no', 'seg_bgn', 
                      'seg_end', 'seg_no', 'side_ind', 'seg_lngth_', 
                      'seg_lngth_2','seq_no', 'fkey', 'fseg', 'flength', 
                      'fdir', 'foffset', 'terrain_ty', 'fgrade', 'c_offset_b',
                      'cum_offset', 'c_offset_1', 'cum_offs_1', 'nlf_cntl_b', 
                      'nlf_cntl_e', 'dir_ind', 'fac_type', 'geometry']]
    return subset_gdf


def read_subset_dat(grade_gdf):
    """
    Parameters
    ----------
    grade_gdf

    Returns
    -------

    """
    grade_gdf_asc_sort = (
        grade_gdf
            .loc[lambda x: x.seg_no.astype(int) % 2 == 0]
            .sort_values(by=['name','cty_code', 'fkey'], ascending=[True, True, True])
            .reset_index(drop=True)
    )
    grade_gdf_desc_sort = (
        grade_gdf
            .loc[lambda x: x.seg_no.astype(int) % 2 != 0]
            .sort_values(by=['name','cty_code', 'fkey'], ascending=[True, True, False])
            .reset_index(drop=True)
    )
    return grade_gdf_asc_sort, grade_gdf_desc_sort


def save_subset_dat_by_dir(grade_gdf_asc_sort, grade_gdf_desc_sort, path_processed_data):
    """
    Parameters
    ----------
    grade_gdf_asc_sort
    grade_gdf_desc_sort
    path_processed_data

    Returns
    -------

    """
    if not os.path.exists(os.path.join(path_processed_data, "grade_gdf_asc_sort")):
        os.mkdir(os.path.join(path_processed_data, "grade_gdf_asc_sort"))
    if not os.path.exists(os.path.join(path_processed_data, "grade_gdf_desc_sort")):
        os.mkdir(os.path.join(path_processed_data, "grade_gdf_desc_sort"))
    grade_gdf_asc_sort_outfile = os.path.join(path_processed_data, "grade_gdf_asc_sort", "grade_gdf_asc_sort.shp")
    grade_gdf_desc_sort_outfile = os.path.join(path_processed_data, "grade_gdf_desc_sort", "grade_gdf_desc_sort.shp")
    grade_gdf_asc_sort.to_file(driver='ESRI Shapefile', filename=grade_gdf_asc_sort_outfile)
    grade_gdf_desc_sort.to_file(driver='ESRI Shapefile', filename=grade_gdf_desc_sort_outfile)
    return 1

def read_subset_dat_by_dir(path_processed_data, shapefile_nm = "grade_gdf_asc_sort"):
    input_file = os.path.join(path_processed_data, shapefile_nm, f"{shapefile_nm}.shp")
    gdf = gpd.read_file(filename=input_file)
