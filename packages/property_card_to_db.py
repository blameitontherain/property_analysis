#!/usr/bin/python3
                                                                              #
                                                                              #
## import libraries
import db_manager, os, fnmatch, pprint, pickle, json
from bs4 import BeautifulSoup
import json
                                                                              #
                                                                              #
## define class to extract data from html files and place into database
class PropertyDataToDB(object):
    ## initialize through-variables
    def __init__(self, jurisdiction):
        ## variable of jurisdiction string
        self.g_juris = jurisdiction
        ## pickle of filenames
        self.filenames_pickle = "../data/bartlett_file_names.pickle"
        ## filename list from pickle
        with open(self.filenames_pickle, "rb") as self.filenames_pickle:
            self.fn_pick = pickle.load(self.filenames_pickle)
        ## path to html files for parsing
        self.html_files_path = "../../all_property_card_html_files/"
        ## path to json dictionary of property span attributes file
        self.prop_attrs_json = "../data/prop_attrs_dict.json"
        ## context manager to open property attributes json dictionary file
        with open(self.prop_attrs_json, "r") as self.prop_attrs_dict:
            ## dictionary of property attributes
            self.prop_attrs_dict = json.load(self.prop_attrs_dict)
        ## pprint instance
        self.pp = pprint.PrettyPrinter(indent=1)
        ## add MySQL database connection credentials
        ## path to database redentials
        self.mysql_credentials_path = "../../credentials/credentials.json"
        ## context manager to create db_manager instance using cred file
        with open(self.mysql_credentials_path, "r") as self.mysql_creds:
            self.mysql_creds = json.load(self.mysql_creds)
            self.mysql_credentials = self.mysql_creds.get("mysql")
            self.username = self.mysql_credentials.get("username")
            self.password = self.mysql_credentials.get("password")
            self.host = self.mysql_credentials.get("host")
            self.database = self.mysql_credentials.get("database")
        ## instance of db manager
        self.db_connection = db_manager.DatabaseManager(self.username, 
                                                        self.password, 
                                                        self.host, 
                                                        self.database)
                                                                              #
                                                                              #
    ## function to begin parsing data within html files
    def parse_html_files(self):
        ## empty list into which property card data dictionaries will go
        self.property_data_list = []
        ## loop for iterating over all files in html file folder
        for self.html in self.fn_pick:
            self.empty_property_dict = {}
            ## context manager to open each file in html folder
            with open(self.html_files_path + self.html, "r") as self.h_file:
                ## file-specific soup instance for parsing property data
                self.soup = BeautifulSoup(self.h_file, "html.parser")
                ## property specific string of each file's property class
                self.property_class = self.soup.find(id="spnPropertyClass")
                self.property_class = self.property_class.text.strip()
                for self.span_k, self.span_v in self.prop_attrs_dict.items():
                    #print(self.span_syntax)
                    try:
                        self.span_text = self.soup.find(id=self.span_v).text
                        self.span_text = self.span_text.strip()
                        self.span_text = self.span_text.replace("$", "")
                        self.span_text = self.span_text.replace(",","")
                        self.empty_property_dict.update(
                                {self.span_k: self.span_text}
                                )
                    except(TypeError, KeyError, AttributeError) as e:
                        self.empty_property_dict.update(
                                {self.span_k: ''}
                                )
            self.property_data_list.append(self.empty_property_dict)
        #print(self.property_data_list)
        ## optional pprint of cumulative property data dictionaries list
        #self.pp.pprint(self.property_data_list)
        ## variable jurisdiction-specific property data filename for pickle
        self.property_deets_fn = self.g_juris + "_property_details.pickle"
        self.prop_data_pick = "../data/" + self.property_deets_fn
        ## context manager for creating a backup of property data as pickle
        with open(self.prop_data_pick, "wb") as property_deets_file:
            pickle.dump(self.property_data_list, property_deets_file)
        return self.property_data_list
                                                                              #
                                                                              #
    def place_into_db(self):
        self.insert_statement = "INSERT INTO " + self.g_juris + '_property_details' + """ (parcel_id, 
                                address, jurisdiction, neighborhood, 
                                tax_map_page, land_square_feet, land_acres,
                                land_dimensions, subdivision_name,
                                subdivision_lot, plat_book_page,
                                number_of_improvements, owner_name, in_care_of,
                                owner_address, owner_city,owner_state,
                                owner_zip, property_class, land_appraisal,
                                building_appraisal, total_appraisal,
                                total_assessment, greenbelt_land, 
                                homesite_land, homesite_building,
                                greenbelt_appraisal, greenbelt_assessment,
                                c_land_use,  c_living_units, c_structure_type, 
                                c_year_built, c_investment_grade, 
                                c_square_footage, stories, exterior_walls, 
                                land_use, year_built, total_rooms, bedrooms,
                                bathrooms, half_baths, basement_type, heat, 
                                fuel, heating_system, fireplace_masonry, 
                                fireplace_prefab, ground_floor_area,
                                total_living_area, car_parking, r_address_1,
                                r_address_2, r_address_3, r_country)
                                VALUES
                                (%(parcel_id)s, %(address)s, %(jurisdiction)s, 
                                %(neighborhood)s, %(tax_map_page)s, 
                                nullif(%(land_square_feet)s, ''), %(land_acres)s,
                                %(land_dimensions)s, %(subdivision_name)s,
                                %(subdivision_lot)s, %(plat_book_page)s,
                                nullif(%(number_of_improvements)s, ''),
                                %(owner_name)s, %(in_care_of)s, 
                                %(owner_address)s, %(owner_city)s, 
                                %(owner_state)s, %(owner_zip)s, 
                                %(property_class)s, %(land_appraisal)s,
                                %(building_appraisal)s, %(total_appraisal)s,
                                %(total_assessment)s, %(greenbelt_land)s,
                                %(homesite_land)s, %(homesite_building)s,
                                %(greenbelt_appraisal)s, 
                                %(greenbelt_assessment)s, %(c_land_use)s, 
                                nullif(%(c_living_units)s, ''), %(c_structure_type)s, 
                                nullif(%(c_year_built)s, ''), %(c_investment_grade)s,
                                nullif(%(c_square_footage)s, ''),
                                nullif(%(stories)s, ''),
                                nullif(%(exterior_walls)s, ''), %(land_use)s,
                                nullif(%(year_built)s, ''), 
                                nullif(%(total_rooms)s, ''),
                                nullif(%(bedrooms)s, ''), 
                                nullif(%(bathrooms)s, ''),
                                nullif(%(half_baths)s, ''), %(basement_type)s,
                                %(heat)s, %(fuel)s, %(heating_system)s,
                                nullif(%(fireplace_masonry)s, ''), 
                                nullif(%(fireplace_prefab)s, ''), 
                                nullif(%(ground_floor_area)s, ''),
                                nullif(%(total_living_area)s, ''),
                                %(car_parking)s, %(r_address_1)s, 
                                %(r_address_2)s, %(r_address_3)s,
                                %(r_country)s)
                               """
        self.db_connection.execute_many(self.insert_statement,
                                        self.property_data_list)
        self.db_connection.commit_changes()
        self.db_connection.close_connection()
                                                                              #
                                                                              #
if __name__ == "__main__":
    property_data_insert = PropertyDataToDB("bartlett")
    property_data_insert.parse_html_files()
    property_data_insert.place_into_db()
