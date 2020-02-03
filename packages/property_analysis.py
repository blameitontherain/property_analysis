import pymysql, db_manager, pprint, json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
                                                                              #
                                                                              #
class PropertyAnalytics(object):
    def __init__(self, jurisdiction):
        ## variable of jurisdiction string
        self.g_juris = jurisdiction
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
        self.table = self.g_juris + "_property_details"
        #pd.set_option('precision', 1)
        self.analytics_path = "../analytics/"
                                                                              #
                                                                              #
    def df_from_sql(self, data_list):
        self.data_list = data_list
        self.data_list = ",".join(map(str, self.data_list))
        sql_statement = "SELECT " + self.data_list + " FROM " + self.table 
        #self.dataset = self.db_connection.execute_sql(sql_statement)
        #self.dataset = self.db_connection.execute_fetchall(sql_statement)
        self.dataset = pd.read_sql(sql_statement, 
                                   self.db_connection.return_connection())
        return self.dataset
                                                                              #
                                                                              #
    ## output bar chart with u
    def appraisal_by_year(self, start_range=None, end_range=None):
        self.df = pd.DataFrame(self.dataset)
        self.df_dropped = self.df.dropna()
        self.df_as_int = self.df_dropped.astype(int)
        #self.df = sns.load_dataset(self.df_dropped)
        #print(self.df)

        #self.df_as_int = self.df_dropped.astype(int)
        self.df_grouped = self.df_as_int.groupby(['year_built'])
        print(self.df_grouped.dtypes)
        #print(self.df_grouped.describe())
        self.new_figure = plt.figure()
        self.axes = self.new_figure.add_axes([0,0,1,1])
        #self.violin_plot = self.axes.violinplot(self.df_grouped)
        #self.violin_plot = sns.violinplot(x="year built", y="total appraisal", hue="smoker", data=self.df, palette="Pastel1")

        #plt.show()
        #print(self.dataframe.dtypes)
        #print(self.dataframe.get_group('1995'))
        #print(self.df.head(5))
        #print(self.dataframe.first())
        #self.appraisal_by_date = self.analytics_path + "appraisal_by_date.png"
        #self.violin_plot.savefig(self.appraisal_by_date)


if __name__ == "__main__":
    new_analysis = PropertyAnalytics('bartlett')
    new_analysis.df_from_sql(["total_appraisal", "year_built"])
    new_analysis.appraisal_by_year()
