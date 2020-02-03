#!/usr/bin/python3
                                                                              #
                                                                              #
## import libraries
import db_manager, json
                                                                              #
                                                                              #
## define path to database credentials json file
## adjust your file path as necessary
credentials_path = "../../credentials/credentials.json"
                                                                              #
                                                                              #
class TableMaker(object):
    def __init__(self, jurisdiction):
        self.g_juris = jurisdiction
        ## open the database credentials json file
        with open(credentials_path, "r") as self.creds_file:
            ## create a json object for accessing credentials
            self.credentials_json = json.load(self.creds_file)
            ## access the parent dictionary object
            self.credentials_dict = self.credentials_json.get("mysql")
        ## define credential variables for mysql DatabaseManager class/object
        self.username = self.credentials_dict.get("username")
        self.password = self.credentials_dict.get("password")
        self.host = self.credentials_dict.get("host")
        self.database = self.credentials_dict.get("database")
        ## Create new DatabaseManager Object for targeted database
        self.new_db_connection = db_manager.DatabaseManager(self.username, 
                                                            self.password, 
                                                            self.host, 
                                                            self.database)
                                                                              #
                                                                              #
    def create_master_table(self):
        self.new_table_name = self.g_juris + "_property_details"
        self.create_table = "CREATE TABLE IF NOT EXISTS " + \
                            self.new_table_name + """(
                            parcel_id VARCHAR(40), address VARCHAR(100),
                            jurisdiction VARCHAR(50), neighborhood VARCHAR(50),
                            tax_map_page VARCHAR(30), 
                            land_square_feet FLOAT(13,2),
                            land_acres FLOAT(13,2),
                            land_dimensions VARCHAR(50),
                            subdivision_name VARCHAR(100),
                            subdivision_lot VARCHAR(25),
                            plat_book_page VARCHAR(25),
                            number_of_improvements INT,
                            owner_name VARCHAR(255), in_care_of VARCHAR(255),
                            owner_address VARCHAR(255), 
                            owner_city VARCHAR(255), owner_state VARCHAR(50),
                            owner_zip VARCHAR(20), property_class VARCHAR(50),
                            land_appraisal DECIMAL(13,2),
                            building_appraisal DECIMAL(13,2),
                            total_appraisal DECIMAL(13,2),
                            total_assessment DECIMAL(13,2),
                            greenbelt_land DECIMAL(13,2),
                            homesite_land DECIMAL(13,2),
                            homesite_building DECIMAL(13,2),
                            greenbelt_appraisal DECIMAL(13,2),
                            greenbelt_assessment DECIMAL(13,2),
                            c_land_use VARCHAR(100), c_living_units INT,
                            c_structure_type VARCHAR(100), c_year_built INT,
                            c_investment_grade VARCHAR(50),
                            c_square_footage FLOAT(13,2), stories INT,
                            exterior_walls VARCHAR(50), land_use VARCHAR(100),
                            year_built INT, total_rooms INT,
                            bedrooms INT, bathrooms INT,
                            half_baths INT, basement_type VARCHAR(50),
                            heat VARCHAR(50), fuel VARCHAR(50),
                            heating_system VARCHAR(50),fireplace_masonry INT,
                            fireplace_prefab INT, 
                            ground_floor_area FLOAT(13,2),
                            total_living_area FLOAT(13,2),
                            car_parking VARCHAR(50), r_address_1 VARCHAR(100),
                            r_address_2 VARCHAR(100), r_address_3 VARCHAR(100),
                            r_country VARCHAR(100))
                            """
        ## execute create table statement
        self.new_db_connection.execute_sql(self.create_table)
