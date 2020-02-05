# Bartlett, TN Property Analysis
### This program demonstrates an ongoing data science analysis of properties in Bartlett, TN--a municipal jurisdiction of Shelby County.  
#### The program allows data scientists to parse property data from HTML files and import that data into a MySQL database. With this data, the program utilizes the following python libraries to complete the analysis:
1. pandas
2. scikit
3. json
4. pymysql
5. seaborn/matplotlib
6. geoplotlib
7. beautifulsoup4
#### The visualizations are stored in the `analysis` folder.  
#### If you would like to contribute to this analysis, you may download and use the property card database for Bartlett, TN which is located in the `data` folder.
#### The analysis is written for Python3.

In `packages`:  
I. The file `db_manager.py` is a library for connecting to MySQL with pymysql  
II. The file `create_property_card_table.py` will generate the property card which will house the relevant data for the analysis  
III. The file `property_card_to_db.py` can be used to scrape html file to extract data and insert into the juridiction database defined when the instance is created (these files are not included with this package, but you can contact me if you would like to fork my ongoing Shelby County Assessor of Property scrapy scraper. There is a function for parsing the "easy" files and a function for parsing the "difficult" files. This problem is a result of having a number of element ids based on different property types. Once all ids have been profiled and classified, if/else statements can be used to select which id is appropriate.
IV. the file `html_files_manager.py` is used to 1) create a pickle list of all html filenames, 2) a pickle list of "easy" html filenames, 3) a pickle list of "difficult" html filenames. These files are stored in /data.

In 'data':
1. 'municipal jurisdiction' + '_file_names.pickle' contains the complete list of html filenames for this jurisdiction
2. 'municipal jurisdiction' + '_good_html_files.pickle' contains the complete list of "easy" html filenames
3. 'municipal jurisdiction' + '_bad_html_files.pickle' contains the complete list of "difficult" html filenames

***
