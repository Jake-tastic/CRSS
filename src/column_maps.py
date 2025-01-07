class colmap:
    """
    New column names for accident, vehicle, parkwork, and person
    Use "vehicle" for both vehicle and parkwork files
    Ex. vehicle.columns
    """
    def __init__(self, columns):
        """
        Initialize the colmap with a dictionary of mappings.
        Args:
            columns (dict): Mapping of database column names to source column names.
        Example:
            {
                "db_col1": ["file_col1", "file_col1_alt"],
                "db_col2": ["file_col2"]
            }
        """
        self.columns = columns

    def get_map(self):
        """
        Reverse the mapping: map source column names to database column names.
        Returns:
            dict: A dictionary mapping source file column names to database column names.
        Example:
            {"file_col1": "db_col1", "file_col2": "db_col2"}
        """
        reverse_map = {}
        for key, values in self.columns.items():
            for value in values:
                reverse_map[value] = key
        return reverse_map

    def validate_mapping(self, required_columns):
        missing_columns = [col for col in required_columns if col not in self.columns]
        if missing_columns:
            print(f"WARNING: missing columns in mapping: {missing_columns}")
        return missing_columns

accident = colmap({
    "case_num" : ["casenum"],
    "region" : ["reg"],
    "urbancity" : ["urbanicity"],
    "injuries" : ["no_inj_im"],
    "max_injury" : ["maxsev_im"],
    "pedestrian" : ["peds"],
    "notin_veh" : ["pernotmvit"],
    "vehicles" : ["ve_total"],
    "moving" : ["ve_forms"],
    "not_moving" : ["pvh_invl"],
    "occupants" : ["permvit"],
    "collision" :["man_coll", "manner of collision"],
    "related_junct" : ["reljct2_im"],
    "intersection" : ["typ_int"],
    "roadway" : ["rel_road"],
    "highway" : ["int_hwy"],
    "workzone" : ["wrk_zone"],
    "lighting" : ["lgtcon_im"],
    "weather" : ["weather1", "weather.1"]
})

vehicle = colmap({
    "case_num" : ["casenum"],
    "v_year" : ["mdlyr_im", "pmodyear"],
    "v_make" : ["vpicmake", "pvpicmake"],
    "v_model": ["vpicmodel", "pvpicmodel"],
    "v_body" : ["vpicbodyclass", "pvpicbodyclass"],
    "gvwr" : ["gvwr_to", "pgvwr_to", "pgvwr"],
    "special_v" : ["spec_use", "psp_use"],
    "moving" : ["unittype", "ptype"],
    "occupants" : ["numoccs", "pnumoccs"],
    "injured" : ["numinj_im"],
    "trailer" : ["tow_veh", "ptrailer"],
    "cargo" : ["cargo_bt", "pcargtyp"],
    "hazmat" : ["haz_cno", "phaz_cno"],
    "hzm_spill" : ["haz_rel", "phaz_rel"],
    "veh_speed" : ["trav_sp"],
    "speedlimit" : ["vspd_lim"],
    "damage" :["deformed", "pveh_sev"],
    "most_harm" : ["vevent_im", "pm_harm"],
    "fire" : ["fire_exp", "pfire"],
    "driver_pres" : ["dr_pres"],
    "hitrun" : ["hit_run", "phit_run"],
    "towed" : ["ptowed"],
    "acc_type" : ["ptype"]
})

person = colmap({
    "case_num" : ["casenum"],
    "injury" : ["injsev_im"],
    "seat" : ["seat_im"],
    "eject" : ["eject_im"],
    "alcohol" : ["peralch_im"]
})

if __name__=="__main__":
    print(accident.get_map())