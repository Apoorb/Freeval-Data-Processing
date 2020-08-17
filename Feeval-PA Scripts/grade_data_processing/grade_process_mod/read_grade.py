import geopandas as gpd
import inflection
import os
import pandas as pd
import glob
import fiona
import shutil

def data_read_switch(path_to_data, path_to_grade_data_file,
                     path_processed_data,
                     read_saved_shp_csv=False,
                     read_saved_csv=True):
    """
    Read the appropriate file based on what was saved previously and
    read_shape_file tag.

    Parameters
    ----------
    path_to_data : TYPE
        DESCRIPTION.
    path_to_grade_data_file : TYPE
        DESCRIPTION.
    path_processed_data : TYPE
        DESCRIPTION.
    read_shape_file : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    return_dict : TYPE
        DESCRIPTION.
    """
    if (read_saved_shp_csv 
        & (len(glob.glob(
            os.path.join(path_processed_data,
                             "grade_gdf_asc_sort", "*.shp"))) == 1)
        & len(glob.glob(
            os.path.join(path_processed_data,
                             "grade_gdf_desc_sort", "*.shp"))) == 1):
        # Read the data if above conditions met
        grade_gdf_asc_sort = read_subset_dat_by_dir(
            path_processed_data=path_processed_data,
            shapefile_nm="grade_gdf_asc_sort"
        )
        grade_gdf_desc_sort = read_subset_dat_by_dir(
            path_processed_data=path_processed_data,
            shapefile_nm="grade_gdf_desc_sort"
        )
        grade_df_asc = pd.read_csv(os.path.join(
            path_to_data,
            "processed_data", "grade_asc_data.csv"), index_col=0)
        grade_df_desc = pd.read_csv(os.path.join(
            path_to_data,
            "processed_data", "grade_desc_data.csv"), index_col=0)
        return_dict ={
            "grade_gdf_asc_sort":grade_gdf_asc_sort,
            "grade_gdf_desc_sort":grade_gdf_desc_sort,
            "grade_df_asc":grade_df_asc,
            "grade_df_desc":grade_df_desc
        }
        return return_dict
    elif ~ read_saved_shp_csv:
        fiona.listlayers(path_to_grade_data_file)
        grade_gdf = read_raw_data(
            filename_gdf=path_to_grade_data_file,
            layer_gdf='SpatialJoin_GradeDataFINAL'
        )
        grade_gdf_asc_sort, grade_gdf_desc_sort = create_subset_dat(
            grade_gdf=grade_gdf
        )
        save_subset_dat_by_dir(
            grade_gdf_asc_sort=grade_gdf_asc_sort,
            grade_gdf_desc_sort=grade_gdf_desc_sort,
            path_processed_data=path_processed_data
        )
        grade_df_asc = pd.read_csv(os.path.join(path_processed_data,
                                                "grade_asc_data.csv"),
                                   index_col=0)
        grade_df_desc = pd.read_csv(os.path.join(path_processed_data,
                                                 "grade_desc_data.csv"),
                                    index_col=0)
        return_dict ={
            "grade_gdf_asc_sort":grade_gdf_asc_sort,
            "grade_gdf_desc_sort":grade_gdf_desc_sort,
            "grade_df_asc":grade_df_asc,
            "grade_df_desc":grade_df_desc
        }
        return return_dict
    elif read_saved_csv:
        grade_df_asc = pd.read_csv(os.path.join(path_processed_data,
                                                "grade_asc_data.csv"),
                                   index_col=0)
        grade_df_desc = pd.read_csv(os.path.join(path_processed_data,
                                                 "grade_desc_data.csv"),
                                    index_col=0)
        return_dict ={
            "grade_df_asc":grade_df_asc,
            "grade_df_desc":grade_df_desc
        }
        return return_dict
    else:
        print("")


def read_raw_data(filename_gdf, layer_gdf):
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
                      'seg_lngth_2', 'seq_no', 'fkey', 'fseg', 'flength',
                      'fdir', 'foffset', 'terrain_ty', 'fgrade', 'c_offset_b',
                      'cum_offset', 'c_offset_1', 'cum_offs_1', 'nlf_cntl_b',
                      'nlf_cntl_e', 'dir_ind', 'fac_type', 'geometry']]
    return subset_gdf


def create_subset_dat(grade_gdf):
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
            .sort_values(by=['name', 'cty_code', 'fkey'],
                         ascending=[True, True, True])
            .reset_index(drop=True)
    )
    grade_gdf_desc_sort = (
        grade_gdf
            .loc[lambda x: x.seg_no.astype(int) % 2 != 0]
            .sort_values(by=['name', 'cty_code', 'fkey'],
                         ascending=[True, True, False])
            .reset_index(drop=True)
    )
    return grade_gdf_asc_sort, grade_gdf_desc_sort


def save_subset_dat_by_dir(grade_gdf_asc_sort,
                           grade_gdf_desc_sort, path_processed_data):
    """
    Parameters
    ----------
    grade_gdf_asc_sort
    grade_gdf_desc_sort
    path_processed_data

    Returns
    -------

    """
    if os.path.exists(os.path.join(path_processed_data,
                                       "grade_gdf_asc_sort")):
        shutil.rmtree(os.path.join(path_processed_data,
                              "grade_gdf_asc_sort"))
    os.mkdir(os.path.join(path_processed_data,
                          "grade_gdf_asc_sort"))
    
    if os.path.exists(os.path.join(path_processed_data,
                                       "grade_gdf_desc_sort")):
        shutil.rmtree(os.path.join(path_processed_data,
                              "grade_gdf_desc_sort"))
    os.mkdir(os.path.join(path_processed_data,
                          "grade_gdf_desc_sort"))
    grade_gdf_asc_sort_outfile = os.path.join(path_processed_data,
                                              "grade_gdf_asc_sort",
                                              "grade_gdf_asc_sort.shp")
    grade_gdf_desc_sort_outfile = os.path.join(path_processed_data,
                                               "grade_gdf_desc_sort",
                                               "grade_gdf_desc_sort.shp")
    grade_gdf_asc_sort.to_file(driver='ESRI Shapefile',
                               filename=grade_gdf_asc_sort_outfile)
    grade_gdf_desc_sort.to_file(driver='ESRI Shapefile',
                                filename=grade_gdf_desc_sort_outfile)
    grade_df_asc = pd.DataFrame(grade_gdf_asc_sort.drop(columns='geometry'))
    grade_df_desc = pd.DataFrame(grade_gdf_desc_sort.drop(columns='geometry'))
    grade_df_asc.to_csv(
        os.path.join(path_processed_data, "grade_asc_data.csv"))
    grade_df_desc.to_csv(os.path.join(path_processed_data,
                                      "grade_desc_data.csv"))
    return 1


def read_subset_dat_by_dir(path_processed_data,
                           shapefile_nm="grade_gdf_asc_sort"):
    """
    Parameters
    ----------
    path_processed_data : TYPE
        DESCRIPTION.
    shapefile_nm : TYPE, optional
        DESCRIPTION. The default is "grade_gdf_asc_sort".

    Returns
    -------
    gdf : TYPE
        DESCRIPTION.

    """
    input_file = os.path.join(path_processed_data, shapefile_nm,
                              f"{shapefile_nm}.shp")
    gdf = gpd.read_file(filename=input_file)
    return gdf

