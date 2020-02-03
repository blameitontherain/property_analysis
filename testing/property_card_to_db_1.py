#!/usr/bin/python3


## import libraries
import db_manager, os, fnmatch, pprint, pickle, json
from bs4 import BeautifulSoup


## define class to extract data from html files and place into database
class PropertyDataToDB(object):
    ## initialize through-variables
    def __init__(self, jurisdiction):
        ## variable of jurisdiction string
        self.global_jurisdiction = jurisdiction
        ## path to html files for parsing
        self.path_to_html_files = "../../all_property_card_html_files/"
        ## pprint instance
        self.pp = pprint.PrettyPrinter(indent=1)
        ## add MySQL database connection credentials
        self.mysql_credentials_path = "../../credentials/credentials.json"
        with open(self.mysql_credentials_path, "r") as self.mysql_creds:
            self.mysql_creds = json.load(self.mysql_creds)
            self.mysql_credentials = self.mysql_creds.get('mysql')
            self.username = self.mysql_credentials.get('username')
            self.password = self.mysql_credentials.get('password')
            self.host = self.mysql_credentials.get('host')
            self.database = self.mysql_credentials.get('database')
        self.db_connection = db_manager.DatabaseManager(self.username, self.password, self.host, self.database)
        ## access list of relevant property card files
        self.good_pickle_path = "../data/" + self.global_jurisdiction + "_good_html_files.pickle"
        self.bad_pickle_path = "../data/" + self.global_jurisdiction + "bad_html_files.pickle"


    def parse_html_files(self):
        ## next step is to create a list of dictionaries so I can execute many to
        ## speed up process
        with open(self.good_pickle_path, "rb") as self.pickle_file:
            self.list_of_filenames = list(pickle.load(self.pickle_file))
        self.property_data_list = []
        for self.html in self.list_of_filenames:
            with open(self.path_to_html_files + self.html, "r") as self.html_file:
                self.soup = BeautifulSoup(self.html_file, 'html.parser')
                self.property_class = self.soup.find(id="spnPropertyClass").text.strip()
                if self.property_class == "RESIDENTIAL" and self.soup.find(id="spnOwnerZip").text.strip() != None:
                    print(self.html_file)
                    self.property_data_list.append({'parcel_id': self.soup.find(id="spnParcelID").text.strip(),
                                                'property_address': self.soup.find(id="spnAddress").text.strip(),
                                                'jurisdiction': self.soup.find(id="spnMunicipality").text.strip(),
                                                'neighborhood_number': self.soup.find(id="spnNeighborhood").text.strip(),
                                                'tax_map_page': self.soup.find(id="spnMapPage").text.strip(),
                                                'land_sqft': self.soup.find(id="spnLandSquareFeet").text.strip(),
                                                'acres': self.soup.find(id="spnLandAcres").text.strip(),
                                                'lot_dimensions': self.soup.find(id="spnLandDimensions").text.strip(),
                                                'subdivision_name': self.soup.find(id="spnSubdivisionName").text.strip(),
                                                'subdivision_lot': self.soup.find(id="spnSubdivisionLot").text.strip(),
                                                'plat_book_page': self.soup.find(id="spnPlatBookPage").text.strip(),
                                                'improvements': self.soup.find(id="spnNumberOfImprovements").text.strip(),
                                                'owner_name': self.soup.find(id="spnOwnerName").text.strip(),
                                                'care_of': self.soup.find(id="spnInCareOf").text.strip(),
                                                'owner_address': self.soup.find(id="SpnOwnerAddress").text.strip(),
                                                'owner_city': self.soup.find(id="SpnOwnerCity").text.strip(),
                                                'owner_state': self.soup.find(id="SpnOwnerState").text.strip(),
                                                'owner_zip': self.soup.find(id="SpnOwnerZip").text.strip(),
                                                'property_class': self.soup.find(id="spnPropertyClass").text.strip(),
                                                'land_appraisal': self.soup.find(id="spnLandAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'building_appraisal': self.soup.find(id="spnBuildingAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'total_appraisal': self.soup.find(id="spnTotalAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'total_assessment': self.soup.find(id="spnTotalAssessment").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_land_appraisal': self.soup.find(id="spnGreenbeltLand").text.strip().replace('$', '').replace(',',''),
                                                'homesite_land_appraisal': self.soup.find(id="spnHomesiteLand").text.strip().replace('$', '').replace(',',''),
                                                'homesite_building_appraisal': self.soup.find(id="spnHomesiteBuilding").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_appraisal': self.soup.find(id="spnGreenbeltAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_assessment': self.soup.find(id="spnGreenBeltAssessment").text.strip().replace('$', '').replace(',',''),
                                                'stories': self.soup.find(id="spnStories").text.strip(),
                                                'exterior_walls': self.soup.find(id="spnExteriorWalls").text.strip(),
                                                'land_use': self.soup.find(id="spnLandUse").text.strip().split("- ")[1],
                                                'year_built': self.soup.find(id="spnYearBuilt").text.strip(),
                                                'total_rooms': self.soup.find(id="spnTotalRooms").text.strip(),
                                                'bedrooms': self.soup.find(id="spnBedrooms").text.strip(),
                                                'bathrooms': self.soup.find(id="spnBathrooms").text.strip(),
                                                'half_baths': self.soup.find(id="spnHalfBaths").text.strip(),
                                                'basement_type': self.soup.find(id="spnBasementType").text.strip(),
                                                'heat': self.soup.find(id="spnHeat").text.strip(),
                                                'fuel': self.soup.find(id="spnFuel").text.strip(),
                                                'heating_system': self.soup.find(id="spnHeatingSystem").text.strip(),
                                                'fireplace_masonry': self.soup.find(id="spnFireplaceMasonry").text.strip(),
                                                'fireplace_prefab': self.soup.find(id="spnFireplacePrefab").text.strip(),
                                                'ground_floor_area': self.soup.find(id="spnGroundFloorArea").text.strip(),
                                                'total_living_area': self.soup.find(id="spnTotalLivingArea").text.strip(),
                                                'car_parking': self.soup.find(id="spnCarParking").text.strip(),
                                              })
                elif self.property_class == "COMMERCIAL" or self.property_class == "EXEMPT" or self.property_class == "MULTIPLE" or self.property_class == "UTILITY" or self.property_class == "INDUSTRIAL":
                    print(self.html_file)
                    self.property_data_list.append({'parcel_id': self.soup.find(id="spnParcelID").text.strip(),
                                                'property_address': self.soup.find(id="spnAddress").text.strip(),
                                                'jurisdiction': self.soup.find(id="spnMunicipality").text.strip(),
                                                'neighborhood_number': self.soup.find(id="spnNeighborhood").text.strip(),
                                                'tax_map_page': self.soup.find(id="spnMapPage").text.strip(),
                                                'building_sqft': self.soup.find(id="spnCSquareFootage").text.strip(),
                                                'land_sqft': self.soup.find(id="spnLandSquareFeet").text.strip(),
                                                'acres': self.soup.find(id="spnLandAcres").text.strip(),
                                                'lot_dimensions': self.soup.find(id="spnLandDimensions").text.strip(),
                                                'subdivision_name': self.soup.find(id="spnSubdivisionName").text.strip(),
                                                'subdivision_lot': self.soup.find(id="spnSubdivisionLot").text.strip(),
                                                'plat_book_page': self.soup.find(id="spnPlatBookPage").text.strip(),
                                                'improvements': self.soup.find(id="spnNumberOfImprovements").text.strip(),
                                                'owner_name': self.soup.find(id="spnOwnerName").text.strip(),
                                                'care_of': self.soup.find(id="spnInCareOf").text.strip(),
                                                'owner_address': self.soup.find(id="spnOwnerAddress").text.strip(),
                                                'owner_city': self.soup.find(id="spnOwnerCity").text.strip(),
                                                'owner_state': self.soup.find(id="spnOwnerState").text.strip(),
                                                'owner_zip': self.soup.find(id="spnOwnerZip").text.strip(),
                                                'property_class': self.soup.find(id="spnPropertyClass").text.strip(),
                                                'land_appraisal': self.soup.find(id="spnLandAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'building_appraisal': self.soup.find(id="spnBuildingAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'total_appraisal': self.soup.find(id="spnTotalAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'total_assessment': self.soup.find(id="spnTotalAssessment").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_land_appraisal': self.soup.find(id="spnGreenbeltLand").text.strip().replace('$', '').replace(',',''),
                                                'homesite_land_appraisal': self.soup.find(id="spnHomesiteLand").text.strip().replace('$', '').replace(',',''),
                                                'homesite_building_appraisal': self.soup.find(id="spnHomesiteBuilding").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_appraisal': self.soup.find(id="spnGreenbeltAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_assessment': self.soup.find(id="spnGreenBeltAssessment").text.strip().replace('$', '').replace(',',''),
                                                'investment_grade': self.soup.find(id="spnCInvestmentGrade").text.strip(),
                                                'land_use': self.soup.find(id="spnCLandUse").text.strip().split("- ")[1],
                                                'year_built': self.soup.find(id="spnCYearBuilt").text.strip(),
                                                'living_units': self.soup.find(id="spnCLivingUnits").text.strip(),
                                                'structure_type': self.soup.find(id="spnCStructureType").text.strip(),
                                                'total_rooms': self.soup.find(id="spnTotalRooms").text.strip(),
                                                'bedrooms': self.soup.find(id="spnBedrooms").text.strip(),
                                                'bathrooms': self.soup.find(id="spnBathrooms").text.strip(),
                                                'half_baths': self.soup.find(id="spnHalfBaths").text.strip(),
                                                'basement_type': self.soup.find(id="spnBasementType").text.strip(),
                                                'heat': self.soup.find(id="spnHeat").text.strip(),
                                                'fuel': self.soup.find(id="spnFuel").text.strip(),
                                                'heating_system': self.soup.find(id="spnHeatingSystem").text.strip(),
                                                'fireplace_masonry': self.soup.find(id="spnFireplaceMasonry").text.strip(),
                                                'fireplace_prefab': self.soup.find(id="spnFireplacePrefab").text.strip(),
                                                'ground_floor_area': self.soup.find(id="spnGroundFloorArea").text.strip(),
                                                'total_living_area': self.soup.find(id="spnTotalLivingArea").text.strip(),
                                                'car_parking': self.soup.find(id="spnCarParking").text.strip(),
                                              })
                elif self.property_class == "RESIDENTIAL" and self.soup.find(id="spnOwnerZip").text.strip() == None:
                    print(self.html_file)
                    self.property_data_list.append({'parcel_id': self.soup.find(id="spnParcelID").text.strip(),
                                                'property_address': self.soup.find(id="spnAddress").text.strip(),
                                                'jurisdiction': self.soup.find(id="spnMunicipality").text.strip(),
                                                'neighborhood_number': self.soup.find(id="spnNeighborhood").text.strip(),
                                                'tax_map_page': self.soup.find(id="spnMapPage").text.strip(),
                                                'land_sqft': self.soup.find(id="spnLandSquareFeet").text.strip(),
                                                'acres': self.soup.find(id="spnLandAcres").text.strip(),
                                                'lot_dimensions': self.soup.find(id="spnLandDimensions").text.strip(),
                                                'subdivision_name': self.soup.find(id="spnSubdivisionName").text.strip(),
                                                'subdivision_lot': self.soup.find(id="spnSubdivisionLot").text.strip(),
                                                'plat_book_page': self.soup.find(id="spnPlatBookPage").text.strip(),
                                                'improvements': self.soup.find(id="spnNumberOfImprovements").text.strip(),
                                                'owner_name': self.soup.find(id="spnOwnerName").text.strip(),
                                                'care_of': self.soup.find(id="spnInCareOf").text.strip(),
                                                'address_one': self.soup.find(id="SpnADDR1").text.strip(),
                                                'address_two': self.soup.find(id="SpnADDR2").text.strip(),
                                                'address_three': self.soup.find(id="SpnADDR3").text.strip(),
                                                'owner_country': self.soup.find(id="SpnCountry").text.strip(),
                                                'property_class': self.soup.find(id="spnPropertyClass").text.strip(),
                                                'land_appraisal': self.soup.find(id="spnLandAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'building_appraisal': self.soup.find(id="spnBuildingAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'total_appraisal': self.soup.find(id="spnTotalAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'total_assessment': self.soup.find(id="spnTotalAssessment").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_land_appraisal': self.soup.find(id="spnGreenbeltLand").text.strip().replace('$', '').replace(',',''),
                                                'homesite_land_appraisal': self.soup.find(id="spnHomesiteLand").text.strip().replace('$', '').replace(',',''),
                                                'homesite_building_appraisal': self.soup.find(id="spnHomesiteBuilding").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_appraisal': self.soup.find(id="spnGreenbeltAppraisal").text.strip().replace('$', '').replace(',',''),
                                                'greenbelt_assessment': self.soup.find(id="spnGreenBeltAssessment").text.strip().replace('$', '').replace(',',''),
                                                'stories': self.soup.find(id="spnStories").text.strip(),
                                                'exterior_walls': self.soup.find(id="spnExteriorWalls").text.strip(),
                                                'land_use': self.soup.find(id="spnLandUse").text.strip().split("- ")[1],
                                                'year_built': self.soup.find(id="spnYearBuilt").text.strip(),
                                                'total_rooms': self.soup.find(id="spnTotalRooms").text.strip(),
                                                'bedrooms': self.soup.find(id="spnBedrooms").text.strip(),
                                                'bathrooms': self.soup.find(id="spnBathrooms").text.strip(),
                                                'half_baths': self.soup.find(id="spnHalfBaths").text.strip(),
                                                'basement_type': self.soup.find(id="spnBasementType").text.strip(),
                                                'heat': self.soup.find(id="spnHeat").text.strip(),
                                                'fuel': self.soup.find(id="spnFuel").text.strip(),
                                                'heating_system': self.soup.find(id="spnHeatingSystem").text.strip(),
                                                'fireplace_masonry': self.soup.find(id="spnFireplaceMasonry").text.strip(),
                                                'fireplace_prefab': self.soup.find(id="spnFireplacePrefab").text.strip(),
                                                'ground_floor_area': self.soup.find(id="spnGroundFloorArea").text.strip(),
                                                'total_living_area': self.soup.find(id="spnTotalLivingArea").text.strip(),
                                                'car_parking': self.soup.find(id="spnCarParking").text.strip(),
                                              })



        #self.pp.pprint(self.property_data_list)
        with open("../data/bartlett_easy_property_card_details.pickle", "wb") as property_deets_file:
            pickle.dump(self.property_data_list, property_deets_file)
        return self.property_data_list


    def place_into_db(self):
        self.insert_statement = "INSERT INTO " + 'bartlett' + """ (
                                parcel_id, property_address, jurisdiction,
                                neighborhood_number, tax_map_page, land_sqft,
                                acres, lot_dimensions, subdivision_name, subdivision_lot,
                                plat_book_page, improvements, owner_name, care_of,
                                owner_address, owner_city, owner_state, owner_zip,
                                property_class, land_appraisal, building_appraisal,
                                total_appraisal, total_assessment, greenbelt_land_appraisal,
                                homesite_land_appraisal, homesite_building_appraisal,
                                greenbelt_appraisal, greenbelt_assessment, stories,
                                exterior_walls, land_use, year_built, total_rooms, 
                                bedrooms, bathrooms, half_baths, basement_type, heat, 
                                fuel, heating_system, fireplace_masonry, fireplace_prefab,
                                ground_floor_area, total_living_area, car_parking)
                                VALUES  (%(parcel_id)s, %(property_address)s, 
                                %(jurisdiction)s, %(neighborhood_number)s, 
                                %(tax_map_page)s, %(land_sqft)s, %(acres)s, 
                                %(lot_dimensions)s, %(subdivision_name)s, 
                                %(subdivision_lot)s, %(plat_book_page)s, 
                                nullif(%(improvements)s, ''), %(owner_name)s, %(care_of)s,
                                %(owner_address)s, %(owner_city)s, %(owner_state)s, 
                                %(owner_zip)s, %(property_class)s, %(land_appraisal)s, 
                                %(building_appraisal)s, %(total_appraisal)s, 
                                %(total_assessment)s, %(greenbelt_land_appraisal)s,
                                %(homesite_land_appraisal)s, 
                                %(homesite_building_appraisal)s, %(greenbelt_appraisal)s, 
                                %(greenbelt_assessment)s, nullif(%(stories)s, ''), %(exterior_walls)s, 
                                %(land_use)s, nullif(%(year_built)s, ''), nullif(%(total_rooms)s, ''), 
                                nullif(%(bedrooms)s, ''), nullif(%(bathrooms)s, ''), nullif(%(half_baths)s, ''), 
                                %(basement_type)s, %(heat)s, %(fuel)s, 
                                %(heating_system)s, %(fireplace_masonry)s, 
                                %(fireplace_prefab)s, nullif(%(ground_floor_area)s, ''), 
                                nullif(%(total_living_area)s, ''), %(car_parking)s) 
                                """
        self.db_connection.execute_many(self.insert_statement, self.property_data_list)
        self.db_connection.commit_changes()
        self.db_connection.close_connection()


if __name__ == "__main__":
    property_data_insert = PropertyDataToDB('bartlett')
    property_data_insert.parse_html_files()
    #property_data_insert.place_into_db()
