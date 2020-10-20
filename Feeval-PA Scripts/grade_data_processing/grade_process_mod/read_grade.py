import geopandas as gpd
import inflection
import os
import pandas as pd
import glob

import fiona
import shutil

# FIXME: Add documentation
class ReadGrade:
    def __init__(
        self,
        path_to_data,
        path_to_grade_data_file,
        path_interim,
        layers_raw_grade_data=["SpatialJoin_GradeDataFINAL"],
        read_saved_shp_csv=False,
        read_saved_csv=True,
    ):
        self.path_to_data = path_to_data
        self.path_interim = path_interim
        self.path_to_grade_data_file = path_to_grade_data_file
        self.layers_raw_grade_data = layers_raw_grade_data
        self.read_saved_shp_csv = read_saved_shp_csv
        self.read_saved_csv = read_saved_csv

    def data_read_switch(
        self,
        grade_gdf_asc_save_loc="grade_gdf_asc_sort",
        grade_gdf_desc_save_loc="grade_gdf_desc_sort",
    ):
        if (
            self.read_saved_shp_csv
            & (
                len(
                    glob.glob(
                        os.path.join(self.path_interim, grade_gdf_asc_save_loc, "*.shp")
                    )
                )
                == 1
            )
            & len(
                glob.glob(
                    os.path.join(self.path_interim, grade_gdf_desc_save_loc, "*.shp")
                )
            )
            == 1
        ):
            # Read the data if above conditions met
            grade_gdf_asc_sort = self.read_subset_dat_by_dir(
                shapefile_nm=grade_gdf_asc_save_loc
            )
            grade_gdf_desc_sort = self.read_subset_dat_by_dir(
                shapefile_nm=grade_gdf_desc_save_loc
            )
            grade_df_asc = pd.read_csv(
                os.path.join(
                    self.path_to_data, "processed_data", f"{grade_gdf_asc_save_loc}.csv"
                ),
                index_col=0,
            )
            grade_df_desc = pd.read_csv(
                os.path.join(
                    self.path_to_data,
                    "processed_data",
                    f"{grade_gdf_desc_save_loc}.csv",
                ),
                index_col=0,
            )
            return_dict = {
                "grade_gdf_asc_sort": grade_gdf_asc_sort,
                "grade_gdf_desc_sort": grade_gdf_desc_sort,
                "grade_df_asc": grade_df_asc,
                "grade_df_desc": grade_df_desc,
            }
            return return_dict
        elif not self.read_saved_csv:
            if (
                len(
                    glob.glob(
                        os.path.join(self.path_interim, grade_gdf_asc_save_loc, "*.shp")
                    )
                )
                == 1
            ) & (
                len(
                    glob.glob(
                        os.path.join(
                            self.path_interim, grade_gdf_desc_save_loc, "*.shp"
                        )
                    )
                )
                == 1
            ):
                # if block begins
                y_n = input(
                    "Directly reading GIS geodatabase is "
                    "expensive."
                    " You have smaller subset of data saved as "
                    ".shp. and .csv.\n"
                    "Do you want to continue this read "
                    "operation: (Y/N)"
                )
                if y_n.upper() == "N":
                    print("Use the saved .shp or .csv file parameters!")
                    return
                elif y_n.upper() == "Y":
                    print(
                        "Be paitent. Processing raw geodatabase will take 30-40 mins."
                    )
                elif y_n.upper() != "N":
                    raise ValueError("Enter Y or N next time!")
            fiona.listlayers(self.path_to_grade_data_file)
            grade_gdf = self.read_raw_data()
            grade_gdf_asc_sort, grade_gdf_desc_sort = self.create_subset_dat(
                grade_gdf=grade_gdf
            )
            self.save_subset_dat_by_dir(
                grade_gdf_asc_sort=grade_gdf_asc_sort,
                grade_gdf_desc_sort=grade_gdf_desc_sort,
                grade_gdf_asc_save_loc=grade_gdf_asc_save_loc,
                grade_gdf_desc_save_loc=grade_gdf_desc_save_loc,
            )
            grade_df_asc = pd.read_csv(
                os.path.join(self.path_interim, f"{grade_gdf_asc_save_loc}.csv"),
                index_col=0,
            )
            grade_df_desc = pd.read_csv(
                os.path.join(self.path_interim, f"{grade_gdf_desc_save_loc}.csv"),
                index_col=0,
            )
            return_dict = {
                "grade_gdf_asc_sort": grade_gdf_asc_sort,
                "grade_gdf_desc_sort": grade_gdf_desc_sort,
                "grade_df_asc": grade_df_asc,
                "grade_df_desc": grade_df_desc,
            }
            return return_dict
        elif self.read_saved_csv:
            grade_df_asc = pd.read_csv(
                os.path.join(self.path_interim, f"{grade_gdf_asc_save_loc}.csv"),
                index_col=0,
            )
            grade_df_desc = pd.read_csv(
                os.path.join(self.path_interim, f"{grade_gdf_desc_save_loc}.csv"),
                index_col=0,
            )
            return_dict = {"grade_df_asc": grade_df_asc, "grade_df_desc": grade_df_desc}
            return return_dict
        else:
            print("")

    def read_raw_data(self):
        """Directly read the GIS geodatabase. Expensive operation."""
        list_gdf = []
        for layer_ in self.layers_raw_grade_data:
            list_gdf.append(
                gpd.read_file(filename=self.path_to_grade_data_file, layer=layer_)
            )
        gdf = pd.concat(list_gdf)
        gdf.columns = [inflection.underscore(colname) for colname in gdf.columns]
        subset_gdf = gdf.filter(
            items=[
                "name",
                "juris",
                "cty_code",
                "st_rt_no",
                "seg_bgn",
                "seg_end",
                "seg_no",
                "side_ind",
                "seg_lngth_",
                "seg_lngth_2",
                "seq_no",
                "fkey",
                "fseg",
                "flength",
                "fdir",
                "foffset",
                "terrain_ty",
                "fgrade",
                "c_offset_b",
                "cum_offset",
                "c_offset_1",
                "cum_offs_1",
                "nlf_cntl_b",
                "nlf_cntl_e",
                "dir_ind",
                "fac_type",
                "geometry",
            ]
        )
        return subset_gdf

    def create_subset_dat(self, grade_gdf):
        """
        Create 2 datasets for ascending and descending order from the
        primary geodatabase layer.
        Parameters
        ----------
        grade_gdf: gpd.GeoDataFrame()
        Primary geodatabase layer with all data.
        Returns
        -------
        grade_gdf_asc_sort: gpd.GeoDataFrame()

        grade_gdf_desc_sort
        """
        grade_gdf_asc_sort = (
            grade_gdf.loc[lambda x: x.seg_no.astype(int) % 2 == 0]
            .sort_values(by=["name", "fseg", "foffset"], ascending=[True, True, True])
            .reset_index(drop=True)
        )
        grade_gdf_desc_sort = (
            grade_gdf.loc[lambda x: x.seg_no.astype(int) % 2 != 0]
            .sort_values(by=["name", "fseg", "foffset"], ascending=[True, False, False])
            .reset_index(drop=True)
        )
        return grade_gdf_asc_sort, grade_gdf_desc_sort

    def save_subset_dat_by_dir(
        self,
        grade_gdf_asc_sort,
        grade_gdf_desc_sort,
        grade_gdf_asc_save_loc="grade_gdf_asc_sort",
        grade_gdf_desc_save_loc="grade_gdf_desc_sort",
    ):
        """
        Parameters
        ----------
        grade_gdf_asc_sort
        grade_gdf_desc_sort

        Returns
        -------

        """
        if os.path.exists(os.path.join(self.path_interim, grade_gdf_asc_save_loc)):
            shutil.rmtree(os.path.join(self.path_interim, grade_gdf_asc_save_loc))
        os.mkdir(os.path.join(self.path_interim, grade_gdf_asc_save_loc))

        if os.path.exists(os.path.join(self.path_interim, grade_gdf_desc_save_loc)):
            shutil.rmtree(os.path.join(self.path_interim, grade_gdf_desc_save_loc))
        os.mkdir(os.path.join(self.path_interim, grade_gdf_desc_save_loc))
        grade_gdf_asc_sort_outfile = os.path.join(
            self.path_interim, grade_gdf_asc_save_loc, f"{grade_gdf_asc_save_loc}.shp",
        )
        grade_gdf_desc_sort_outfile = os.path.join(
            self.path_interim,
            grade_gdf_desc_save_loc,
            f"{grade_gdf_desc_save_loc}.shp",
        )
        grade_gdf_asc_sort.to_file(
            driver="ESRI Shapefile", filename=grade_gdf_asc_sort_outfile
        )
        grade_gdf_desc_sort.to_file(
            driver="ESRI Shapefile", filename=grade_gdf_desc_sort_outfile
        )
        grade_df_asc = pd.DataFrame(grade_gdf_asc_sort.drop(columns="geometry"))
        grade_df_desc = pd.DataFrame(grade_gdf_desc_sort.drop(columns="geometry"))
        grade_df_asc.to_csv(
            os.path.join(self.path_interim, f"{grade_gdf_asc_save_loc}.csv")
        )
        grade_df_desc.to_csv(
            os.path.join(self.path_interim, f"{grade_gdf_desc_save_loc}.csv")
        )
        return 1

    def read_subset_dat_by_dir(self, shapefile_nm="grade_gdf_asc_sort"):
        """

        Parameters
        ----------
        shapefile_nm

        Returns
        -------

        """
        input_file = os.path.join(
            self.path_interim, shapefile_nm, f"{shapefile_nm}.shp"
        )
        gdf = gpd.read_file(filename=input_file)
        return gdf
