import warnings
warnings.filterwarnings('ignore') 
import matplotlib.pyplot as plt
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

def aggregate ( dataframe,  
                aggregate_by, 
                to_aggregate,
                method ): 

  if method == 'sum': 

    df_short = dataframe\
             .orderBy(aggregate_by)\
             .groupBy(aggregate_by)\
             .agg(sum(to_aggregate))\
             .collect()
             
    new_to_aggregate = f'sum({to_aggregate})'

  elif method == 'avg': 

    df_short = dataframe\
             .orderBy(aggregate_by)\
             .groupBy(aggregate_by)\
             .agg(avg(to_aggregate))\
             .collect()
             
    new_to_aggregate = f'avg({to_aggregate})'

  else: 

    print('No valid method') 
    return   

  X = [df_short[i][aggregate_by]  
       for i in range(len(df_short))] 

  Y = [df_short[i][new_to_aggregate]
       for i in range(len(df_short))] 

  return X, Y 

def time_series(TITLE,
                YLABEL,
                XLABEL,
                PNGFILE,
                X,
                Y): 

  plt.figure(figsize=(25,15))
  plt.plot(X,Y,linestyle='-',color='b') 
  plt.title(TITLE,fontsize=24,fontweight='bold') 
  plt.xticks(fontsize=18,rotation=45)
  plt.yticks(fontsize=18)
  plt.ylabel(YLABEL,fontsize=24,fontweight='bold')
  plt.xlabel(XLABEL,fontsize=24,fontweight='bold')
  plt.savefig(PNGFILE,bbox_inches='tight') 
  
def scatter(TITLE,
            YLABEL,
            XLABEL,
            PNGFILE,
            X,
            Y): 

  plt.figure(figsize=(25,15))
  plt.scatter(X,Y)
  plt.xlabel(XLABEL,fontsize=24,fontweight='bold') 
  plt.ylabel(YLABEL,fontsize=24,fontweight='bold') 
  plt.title(TITLE,fontsize=24,fontweight='bold') 
  plt.xticks(fontsize=24)
  plt.yticks(fontsize=24)
  plt.savefig(PNGFILE) 

def double_y (TITLE,
              Y1LABEL,
              Y2LABEL,
              XLABEL,
              PNGFILE,
              X,
              Y1,
              Y2): 

  fig,ax1 = plt.subplots(figsize=(25,15))
  ax1.plot(X,Y1,linestyle='-',color='r')
  ax1.set_ylabel(Y1LABEL,fontsize=24,
                 fontweight='bold',color='r')
  ax1.set_xlabel(XLABEL,fontsize=24,
                 fontweight='bold',color='k') 
  ax1.tick_params(axis='y',color='r')
  ax1.set_title(TITLE,fontsize=24,fontweight='bold')
  ax1.tick_params(axis='x', labelsize=20)
  ax1.tick_params(axis='y', labelsize=20)

  ax2 = ax1.twinx()
  ax2.plot(X,Y2,linestyle='--',color='b') 
  ax2.set_ylabel(Y2LABEL,fontsize=24,
                 fontweight='bold',color='b') 
  ax2.tick_params(axis='y',color='b')  
  ax2.tick_params(axis='x', labelsize=20)
  ax2.tick_params(axis='y', labelsize=20)

  ax2.spines['left'].set_color('r')
  ax2.spines['right'].set_color('b')

  plt.savefig(PNGFILE,bbox_inches='tight')
  
findspark.init()

spark = SparkSession\
        .builder\
        .appName('Covid Data Mining')\
        .getOrCreate()

file = 'owid-covid-data.csv'
df = spark.read.csv(file,
                    header = True, 
                    inferSchema = True) 

df.select(['location',
    'continent',
    'date',
    'total_cases',
    'new_cases_per_million',
    'new_deaths_per_million',
    'total_deaths_per_million',
    'new_vaccinations',
    'hosp_patients_per_million',
    'people_vaccinated_per_hundred', 
    'gdp_per_capita']).printSchema()

df.select(['location',
    'continent',
    'date',
    'total_cases',
    'new_cases_per_million',
    'new_deaths_per_million',
    'total_deaths_per_million',
    'new_vaccinations',
    'hosp_patients_per_million',
    'people_vaccinated_per_hundred', 
    'gdp_per_capita']).show(5)

continents = df.sort('continent')\
.select('continent')\
.distinct()\
.show()

countries = df.sort('location')\
.select('location')\
.distinct()\
.collect() 

for i in countries: 
    if 'income' in i['location']: 
        print(i)

df = df.dropna(subset=['continent'])
countries = df.sort('location')\
.select('location')\
.distinct()\
.collect()

for i in countries: 
    if 'income' in i['location']: 
        print(i)

df = df.fillna({'new_vaccinations_smoothed': 0, 
                'hosp_patients': 0,
                'new_cases_smoothed': 0,
                'new_deaths_smoothed': 0,
                'male_smokers': 0, 
                'female_smokers': 0}) 

if df.count() != \
   df.select(['continent','location','date'])\
     .distinct().count(): 
    
        print('There are duplicates') 
    
else: 

        print('There are no duplicates') 


df = df\
.select('*',
       (df['male_smokers'] + df['female_smokers'])\
        .alias('smokers'))

df = df\
.select('*',
      (df['new_vaccinations']/df['population']*100)\
      .alias('percentage_vaccinated')) 

"""
1. Where did COVID-19 begin? 
"""
df_short = df\
.select('location','date','total_cases')\
.filter(df['total_cases']>1000) 

countries = df_short.select('location').distinct().collect()
countries = [countries[i][0] for i in range(len(countries))]

COUNTRY = []
DATE = []
for i, country in enumerate(countries): 
    # Filter by country, sort by date, take the earliest date 
    df_new = df_short\
        .filter(df_short['location']==country)\
        .sort('date').head(1) 
    # Save name of coutry, and earliest date where 
    #   total_cases > 1000, to COUNTRY and DATE lists
    COUNTRY.append(country)
    DATE.append(df_new[0]['date']) 
    print(f'{i},{len(countries)}')

spark_country_date = SparkSession\
    .builder.appName('CountryDate').getOrCreate()
columns = ['Country','Date'] 
country_date = spark_country_date\
    .createDataFrame(data=zip(COUNTRY,DATE),schema=columns)

print(country_date.orderBy('Date').show(10))

print(country_date.orderBy('Date',ascending=False).show(10))

"""
2. How fast did COVID-19 spread throughout the globe? 
"""
X, Y = aggregate(df,'date','new_cases_smoothed','sum') 

time_series('Total New COVID Cases (Smoothed)vs. Date', 
            'Total New Cases (Smoothed) / Millions', 
            'Date (Daily)',
            'Total_New_Cases_vs_Date.png',
            X,
            Y)

"""
3. How many deaths did COVID-19 cause during the pandemic?
"""
X, Y = aggregate(df,'date','new_deaths_smoothed','sum')

time_series('Total New Deaths (Smoothed) vs. Date',
            'Total New Deaths (Smoothed)',
            'Date (Daily)',
            'Total_New_Deaths_vs_Date.png',
            X,
            Y
            )

"""
4. Do poor countries (low GDP) suffer more 
deaths per million from COVID ? 
"""

X, gdp_per_capita = aggregate(df,
            'location',
            'gdp_per_capita',
            'avg')

X, new_deaths_per_million = aggregate(df,
            'location',
            'new_deaths_per_million',
            'sum') 

scatter('GDP PER CAPITA VS. NEW DEATHS PER MILLION',
        'NEW DEATHS PER MILLION',
        'GDP PER CAPITA',
        'GDP_PER_CAPITA_VS_NEW_DEATHS_PER_MILLION.png',
        gdp_per_capita,
        new_deaths_per_million)

"""
5. Does diabetes increase deaths from COVID-19? 
"""

X, new_deaths_smoothed = aggregate(df,
                         'location',
                         'new_deaths_smoothed',
                         'sum') 

X, diabetes_prevalence = aggregate(df,
                         'location',
                         'diabetes_prevalence',
                         'avg') 

scatter('Diabetes Prevalence vs. New Deaths by COVID',
        'New Deaths (smoothed)',
        'Diabetes prevalence (% of population)',
        'Diabetes_Prevalence_vs_New_Deaths.png',
        diabetes_prevalence,
        new_deaths_smoothed) 

"""
6. Does smoking increase risk of death from COVID-19?
"""

X, smokers = aggregate(df,
                       'location',
                       'smokers',
                       'avg') 

scatter('Smokers vs. New Deaths by COVID',
        'New Deaths (smoothed)',
        'Smokers (% of population)',
        'Smokers_vs_New_Deaths.png',
        smokers,
        new_deaths_smoothed) 

"""
7. Does cardiovascular disease increases 
   deaths_per_million from COVID ? 
"""
X, cardiovasc_death_rate = aggregate(df,
                       'location',
                       'cardiovasc_death_rate',
                       'avg') 

scatter('Cardiovascular Disease Death Rate \
vs. New Deaths by COVID',
'New Deaths (smoothed)',
'Death Rate from Cardiovascular Disease \
(per 100,000 people)',
'Cardio_vs_New_Deaths.png',
cardiovasc_death_rate,
new_deaths_smoothed)

"""
8. Does age increase deaths_per_million from COVID ?
"""

X, median_age = aggregate(df,
                'location',
                'median_age',
                'avg') 

scatter('Median Age vs. New Deaths by COVID',
        'New Deaths (smoothed)',
        'Median Age',
        'Median_Age_vs_New_Deaths.png',
        median_age,
        new_deaths_smoothed) 

"""
9. Does handwashing reduce new_cases from COVID?
"""

X, handwashing_facilities = aggregate(df,
                            'location',
                            'handwashing_facilities',
                            'avg') 

X, new_cases_smoothed = aggregate(df,
                                  'location',
                                  'new_cases_smoothed',
                                  'avg')

scatter('Handwashing Facility Access vs. \
New Cases by COVID',
'New Cases (smoothed)',
'% of Population with Basic Handwashing Facility',
'Handwashing_vs_New_Cases.png',
handwashing_facilities,
new_cases_smoothed) 

"""
10. Does vaccination decrease the rate of 
COVID-19 infections ? 
"""

X, new_cases_smoothed = aggregate(
   df, 
   'date',
   'new_cases_smoothed',
   'sum') 

X, new_vaccinations_smoothed = aggregate(
   df, 
   'date',
   'new_vaccinations_smoothed',
   'sum') 

double_y('New Cases (Smoothed) vs. New Vaccinations (Smoothed)',
         'New Cases (Smoothed) / Millions',
         'New Vaccinations (Smoothed) / Millions',
         'Dates (Daily)',
         'New_Cases_New_Vaccinations.png',
         X,
         new_cases_smoothed,
         new_vaccinations_smoothed) 

"""
11.Does vaccination decrease 
severity of COVID-19 infection rate ?
"""

X, hosp_patients = aggregate(df,
                   'date',
                   'hosp_patients',
                   'sum') 

double_y('New Vaccinations (smoothed) vs. Hospital patients',
'New Vaccinations (smoothed) / Millions',
'Hospital Patients',
'Date (Daily)',
'New_Vaccinations_Hospital_Patients.png',
X,
hosp_patients,
new_vaccinations_smoothed
) 

"""
12. Is vaccine distributed irrespective
of a country's economic standing? 
"""

X, percentage_vaccinated = aggregate(df,
                   'location',
                   'percentage_vaccinated',
                   'avg') 

scatter('Percentage vaccinated vs. GDP per capita',
        'Percentage_vaccinated (%)',
        'gdp_per_capita',
        'Percentage_vaccinated_vs_GDP_per_capita.png',
        gdp_per_capita,
        percentage_vaccinated) 

