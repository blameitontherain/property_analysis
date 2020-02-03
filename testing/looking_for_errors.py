import pprint, pickle, db_manager

with open("../data/bartlett_easy_property_card_details.pickle", "rb") as property_data:
    prop_data = pickle.load(property_data)


pp = pprint.PrettyPrinter(indent=1)


property_details_list = list(prop_data)
pp.pprint(property_details_list[0:10])


new_db_interaction = db_manager.DatabaseManager('richard', '82dT5^8#s', 'localhost', 'assessor')

#for i in range(len(property_details_list)):
#    print(property_details_list[i].get('half_baths'))



def place_into_db():
    insert_statement = "INSERT INTO " + 'bartlett' + """ (
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
    new_db_interaction.execute_many(insert_statement, property_details_list)
    new_db_interaction.commit_changes()
    new_db_interaction.close_connection()

place_into_db()
