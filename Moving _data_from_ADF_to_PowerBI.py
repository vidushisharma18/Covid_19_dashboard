# Databricks notebook source
df_country_wise = spark.read.csv("/mnt/c19reportdatalake/raw/Dataset/country_wise_latest.csv", header=True) //read data//
df_country_wise=(df_country_wise
.withColumnRenamed("Country/Region","Country_Region")
.withColumnRenamed("Confirmed","Confirmed")
.withColumnRenamed("Deaths","Deaths")
.withColumnRenamed("Recovered","Recovered")
.withColumnRenamed("Active","Active")
.withColumnRenamed("New cases","New_cases")
.withColumnRenamed("New deaths","New_deaths")
.withColumnRenamed("New recovered","New_recovered")
.withColumnRenamed("Deaths / 100 Cases","Deaths_per_100_Cases")
.withColumnRenamed("Recovered / 100 Cases","Recovered_per_100_Cases")
.withColumnRenamed("Deaths / 100 Recovered","Deaths_per_100_Recovered")
.withColumnRenamed("Confirmed last week","Confirmed_last_week")
.withColumnRenamed("1 week change","1_week_change")
.withColumnRenamed("1 week % increase","1_week_percent_increase")
.withColumnRenamed("WHO Region","WHO_Region")
)
df_country_wise.write.mode("overwrite").option("overwriteSchema", "true").option("path", "/mnt/Final_Dateset_PowerBI/Country_Wise_Latest").saveAsTable("country_wise_latest") 
df_country_wise.display()

# COMMAND ----------

df_covid_19_clean = spark.read.csv("/mnt/c19reportdatalake/raw/Dataset/covid_19_clean_complete.csv", header=True)
df_covid_19_clean=(df_covid_19_clean
.withColumnRenamed("Province/State","Province_State")
.withColumnRenamed("Country/Region","Country_Region")
.withColumnRenamed("Lat","Lat")
.withColumnRenamed("Long","Long")
.withColumnRenamed("Date","Date")
.withColumnRenamed("Confirmed","Confirmed")
.withColumnRenamed("Deaths","Deaths")
.withColumnRenamed("Recovered","Recovered")
.withColumnRenamed("Active","Active")
.withColumnRenamed("WHO Region","WHO_Region")
)
df_covid_19_clean.write.mode("overwrite").option("overwriteSchema", "true").option("path", "/mnt/Final_Dateset_PowerBI/Covid_19_clean").saveAsTable("covid_19_clean") 
df_covid_19_clean.display()

# COMMAND ----------

df_day_wise = spark.read.csv("/mnt/c19reportdatalake/raw/Dataset/day_wise.csv", header=True)
df_day_wise=(df_day_wise
.withColumnRenamed("Date","Date")
.withColumnRenamed("Confirmed","Confirmed")
.withColumnRenamed("Deaths","Deaths")
.withColumnRenamed("Recovered","Recovered")
.withColumnRenamed("Active","Active")
.withColumnRenamed("New cases","New_cases")
.withColumnRenamed("New deaths","New_deaths")
.withColumnRenamed("New recovered","New_recovered")
.withColumnRenamed("Deaths / 100 Cases","Deaths_per_100_Cases")
.withColumnRenamed("Recovered / 100 Cases","Recovered_per_100_Cases")
.withColumnRenamed("Deaths / 100 Recovered","Deaths_per_100_Recovered")
.withColumnRenamed("No. of countries","No._of_countries")
)
df_day_wise.write.mode("overwrite").option("overwriteSchema", "true").option("path", "/mnt/Final_Dateset_PowerBI/Day_wise").saveAsTable("day_wise") 
df_day_wise.display()

# COMMAND ----------

df_full_grouped = spark.read.csv("/mnt/c19reportdatalake/raw/Dataset/full_grouped.csv", header=True)
df_full_grouped=(df_full_grouped
.withColumnRenamed("Date","Date")
.withColumnRenamed("Country/Region","Country/Region")
.withColumnRenamed("Confirmed","Confirmed")
.withColumnRenamed("Deaths","Deaths")
.withColumnRenamed("Recovered","Recovered")
.withColumnRenamed("Active","Active")
.withColumnRenamed("New cases","New_cases")
.withColumnRenamed("New deaths","New_deaths")
.withColumnRenamed("New recovered","New_recovered")
.withColumnRenamed("WHO Region","WHO_Region")
)
df_full_grouped.write.mode("overwrite").option("overwriteSchema", "true").option("path", "/mnt/Final_Dateset_PowerBI/Full_grouped").saveAsTable("df_full_grouped") 
df_full_grouped.display()

# COMMAND ----------

df_usa_county_wise = spark.read.csv("/mnt/c19reportdatalake/raw/Dataset/usa_county_wise.csv", header=True)
df_usa_county_wise=(df_usa_county_wise
.withColumnRenamed("UID","UID")
.withColumnRenamed("iso2","iso2")
.withColumnRenamed("iso3","iso3")
.withColumnRenamed("code3","code3")
.withColumnRenamed("FIPS","FIPS")
.withColumnRenamed("Admin2","Admin2")
.withColumnRenamed("Province_State","Province_State")
.withColumnRenamed("Country_Region","Country_Region")
.withColumnRenamed("Lat","Lat")
.withColumnRenamed("Long_","Long")
.withColumnRenamed("Combined_Key","Combined_Key")
.withColumnRenamed("Date","Date")
.withColumnRenamed("Confirmed","Confirmed")
.withColumnRenamed("Deaths","Deaths")
)
df_usa_county_wise.write.mode("overwrite").option("overwriteSchema", "true").option("path", "/mnt/Final_Dateset_PowerBI/Usa_county_wise").saveAsTable("usa_county_wise") 
df_usa_county_wise.display()

# COMMAND ----------

df_worldometer_data = spark.read.csv("/mnt/c19reportdatalake/raw/Dataset/worldometer_data.csv", header=True)
df_worldometer_data=(df_worldometer_data
.withColumnRenamed("Country/Region","Country_Region")
.withColumnRenamed("Continent","Continent")
.withColumnRenamed("Population","Population")
.withColumnRenamed("TotalCases","TotalCases")
.withColumnRenamed("NewCases","NewCases")
.withColumnRenamed("TotalDeaths","TotalDeaths")
.withColumnRenamed("NewDeaths","NewDeaths")
.withColumnRenamed("TotalRecovered","TotalRecovered")
.withColumnRenamed("NewRecovered","NewRecovered")
.withColumnRenamed("ActiveCases","ActiveCases")
.withColumnRenamed("Serious,Critical","Serious_Critical")
.withColumnRenamed("Tot Cases/1M pop","Tot_cases_per_M_pop")
.withColumnRenamed("Deaths/1M pop","Deaths_per_M_pop")
.withColumnRenamed("TotalTests","TotalTests")
.withColumnRenamed("Tests/1M pop","Tests_per_M_pop")
.withColumnRenamed("WHO Region","WHO_Region")
)
df_worldometer_data.write.mode("overwrite").option("overwriteSchema", "true").option("path", "/mnt/Final_Dateset_PowerBI/Worldometer_data").saveAsTable("worldometer_data") 
df_worldometer_data.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from country_wise_latest
