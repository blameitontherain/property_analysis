import pickle, os, fnmatch, json
from bs4 import BeautifulSoup


class MakeFilesPickle():
    def __init__(self, jurisdiction):
        self.path_to_html_files = "../../all_property_card_html_files/"
        self.pickle_filename = "../data/" + self.jurisdiction + "html_file_names.pickle"
        ## variable of jurisdiction string
        self.global_jurisdiction = jurisdiction


    def list_of_html_files(self):
        ## Should be updated to apply to non-normal first letters, 
        ## e.g., 'L' for 'Arlington'
        if self.global_jurisdiction == 'memphis':
            self.jurisdiction_letter = '0'
        else:
            self.jurisdiction_letter = self.jurisdiction[0]
        self.search_root = "parcel_"
        self.search_pattern = self.search_root + self.jurisdiction_letter.upper()
        self.list_of_files = [name for name in os.listdir(self.path_to_html_files) if fnmatch.fnmatch(name, self.search_pattern + "*")] 
        with open(self.pickle_filename, 'wb') as self.pick_file:
            pickle.dump(self.list_of_files, self.pick_file)


    def find_error_files(self):
        try:
            with open(self.pickle_filename, "rb") as self.html_filenames_list:
                self.html_filenames_list = list(pickle.load(self.html_filenames_list))
        except Exception as e:
            print("Error trying to open the pickle file containing the list of html filenames: ", e)
        self.error_html_files_list = []
        self.good_html_files_list = []
        for self.html in self.html_filenames_list:
            with open(self.path_to_html_files + self.html, "r") as self.html_file:
                try:
                    self.soup = BeautifulSoup(self.html_file, 'html.parser')
                    self.parcel_id = self.soup.find(id="spnParcelID").text.strip(),
                    self.property_address = self.soup.find(id="spnAddress").text.strip(),
                    self.jurisdiction = self.soup.find(id="spnMunicipality").text.strip(),
                    self.neighborhood_number = self.soup.find(id="spnNeighborhood").text.strip(),
                    self.tax_map_page = self.soup.find(id="spnMapPage").text.strip(),
                    self.land_sqft = self.soup.find(id="spnLandSquareFeet").text.strip(),
                    self.acres = self.soup.find(id="spnLandAcres").text.strip(),
                    self.lot_dimensions = self.soup.find(id="spnLandDimensions").text.strip(),
                    self.subdivision_name = self.soup.find(id="spnSubdivisionName").text.strip(),
                    self.subdivision_lot = self.soup.find(id="spnSubdivisionLot").text.strip(),
                    self.plat_book_page = self.soup.find(id="spnPlatBookPage").text.strip(),
                    self.improvements = self.soup.find(id="spnNumberOfImprovements").text.strip(),
                    self.owner_name = self.soup.find(id="spnOwnerName").text.strip(),
                    self.care_of = self.soup.find(id="spnInCareOf").text.strip(),
                    self.owner_address = self.soup.find(id="spnOwnerAddress").text.strip(),
                    self.owner_city = self.soup.find(id="spnOwnerCity").text.strip(),
                    self.owner_state = self.soup.find(id="spnOwnerState").text.strip(),
                    self.owner_zip = self.soup.find(id="spnOwnerZip").text.strip(),
                    self.property_class = self.soup.find(id="spnPropertyClass").text.strip(),
                    self.land_appraisal = self.soup.find(id="spnLandAppraisal").text.strip().replace('$', '').replace(',',''),
                    self.building_appraisal = self.soup.find(id="spnBuildingAppraisal").text.strip().replace('$', '').replace(',',''),
                    self.total_appraisal = self.soup.find(id="spnTotalAppraisal").text.strip().replace('$', '').replace(',',''),
                    self.total_assessment = self.soup.find(id="spnTotalAssessment").text.strip().replace('$', '').replace(',',''),
                    self.greenbelt_land_appraisal = self.soup.find(id="spnGreenbeltLand").text.strip().replace('$', '').replace(',',''),
                    self.homesite_land_appraisal = self.soup.find(id="spnHomesiteLand").text.strip().replace('$', '').replace(',',''),
                    self.homesite_building_appraisal = self.soup.find(id="spnHomesiteBuilding").text.strip().replace('$', '').replace(',',''),
                    self.greenbelt_appraisal = self.soup.find(id="spnGreenbeltAppraisal").text.strip().replace('$', '').replace(',',''),
                    self.greenbelt_assessment = self.soup.find(id="spnGreenBeltAssessment").text.strip().replace('$', '').replace(',',''),
                    self.stories = self.soup.find(id="spnStories").text.strip(),
                    self.exterior_walls = self.soup.find(id="spnExteriorWalls").text.strip(),
                    self.land_use = self.soup.find(id="spnLandUse").text.strip(),
                    self.year_built = self.soup.find(id="spnYearBuilt").text.strip(),
                    self.total_rooms = self.soup.find(id="spnTotalRooms").text.strip(),
                    self.bedrooms = self.soup.find(id="spnBedrooms").text.strip(),
                    self.bathrooms = self.soup.find(id="spnBathrooms").text.strip(),
                    self.half_baths = self.soup.find(id="spnHalfBaths").text.strip(),
                    self.basement_types = self.soup.find(id="spnBasementType").text.strip(),
                    self.heat = self.soup.find(id="spnHeat").text.strip(),
                    self.fuel = self.soup.find(id="spnFuel").text.strip(),
                    self.heating_system = self.soup.find(id="spnHeatingSystem").text.strip(),
                    self.fireplace_masonry = self.soup.find(id="spnFireplaceMasonry").text.strip(),
                    self.fireplace_prefab = self.soup.find(id="spnFireplacePrefab").text.strip(),
                    self.ground_floor_area = self.soup.find(id="spnGroundFloorArea").text.strip(),
                    self.total_living_area = self.soup.find(id="spnTotalLivingArea").text.strip(),
                    self.car_parking = self.soup.find(id="spnCarParking").text.strip()
                    self.good_html_files_list.append(self.html)
                    self.good_html_files_pickle_path = "../data/" + self.global_jurisdiction + "_good_html_files.pickle"
                    with open(self.good_html_files_pickle_path, "wb") as self.good_pick:
                        pickle.dump(self.good_html_files_list, self.good_pick)
                except Exception as e:
                    self.error_html_files_list.append(self.html)
                    self.bad_html_files_pickle_path = "../data/" + self.global_jurisdiction + "_bad_html_files.pickle"
                    with open(self.bad_html_files_pickle_path, "wb") as self.bad_pick:
                        pickle.dump(self.error_html_files_list, self.bad_pick)


