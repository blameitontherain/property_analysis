import pickle, pprint
from bs4 import BeautifulSoup


pp = pprint.PrettyPrinter(indent=1)

path_to_bad_files = "../data/bartlett_bad_html_files.pickle"

with open(path_to_bad_files, "rb") as bad_file:
    bad_files = list(pickle.load(bad_file))

print(len(bad_files))

utility_example = "B0158   00640L"
industrial_example = "B0158I A00005"
exempt_example = "B0201   00042"
multiple_example = "B0149   00449"
commercial_example = "B0158   00946"
residential_example = "B0157Q A00039"

random_example = "B0157   00065"

file_root = "parcel_"
file_in_use = random_example

html_path = "../../all_property_card_html_files/"

html_file_and_path = html_path + file_root + file_in_use


with open(html_file_and_path, "r") as html_file:
    soup = BeautifulSoup(html_file, "html.parser")

#class_list = soup.findAll("span")
#print("###########################", "random", "######################")
#pp.pprint(class_list)









empty_property_class_list = []
for filename in bad_files:
    soup = BeautifulSoup(open(html_path + filename, "r"), "html.parser")
    property_class = soup.find("span", {"id": "spnPropertyClass"}).text
    class_list = soup.findAll("span")
    if property_class == "RESIDENTIAL":
        print(property_class, filename)
        pp.pprint(class_list)

    if property_class not in empty_property_class_list:
        empty_property_class_list.append((property_class,filename))
    else:
        pass
