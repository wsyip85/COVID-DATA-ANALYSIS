import pandas as pd
import warnings
warnings.filterwarnings('ignore') 
import matplotlib.pyplot as plt

def aggregate ( dataframe,  
                aggregate_by, 
                to_aggregate,
                method ): 

  if method == 'sum': 

    df_short = dataframe\
             .sort_values(aggregate_by)\
             .groupby(aggregate_by)\
             [[to_aggregate]]\
             .sum()\
             .reset_index() 

  elif method == 'avg': 

    df_short = dataframe\
             .sort_values(aggregate_by)\
             .groupby(aggregate_by)\
             [[to_aggregate]]\
             .mean()\
             .reset_index() 
  else: 

    print('No valid method') 

    return   

  X = [df_short.iloc[i][aggregate_by]  
       for i in range(len(df_short))] 

  Y = [df_short.iloc[i][to_aggregate]
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
  
file = 'owid-covid-data.csv'
df = pd.read_csv(file) 

df[['location',
    'continent',
    'date',
    'total_cases',
    'new_cases_per_million',
    'new_deaths_per_million',
    'total_deaths_per_million',
    'new_vaccinations',
    'hosp_patients_per_million',
    'people_vaccinated_per_hundred', 
    'gdp_per_capita']].dtypes

df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d') 
print(f'Date column is now in {df.date.dtype} format')

df[['location',
    'continent',
    'date',
    'total_cases',
    'new_cases_per_million',
    'new_deaths_per_million',
    'total_deaths_per_million',
    'new_vaccinations',
    'hosp_patients_per_million',
    'people_vaccinated_per_hundred', 
    'gdp_per_capita']].head(5)

continents = df['continent'].sort_values().unique()
print(continents)

countries = df['location'].sort_values().unique()
for i in countries: 
    if 'income' in i: 
        print(i)

df = df.dropna(subset=['continent'])
countries = df['location'].sort_values().unique()
for i in countries: 
    if 'income' in i: 
        print(i)

df = df.fillna({'new_vaccinations_smoothed': 0, 
                'hosp_patients': 0,
                'new_cases_smoothed': 0,
                'new_deaths_smoothed': 0,
                'male_smokers': 0, 
                'female_smokers': 0}) 

df['smokers'] = df['male_smokers'] + df['female_smokers']

df['percentage_vaccinated'] = \
df['new_vaccinations'] / df['population'] * 100

df = df\
.drop_duplicates(subset=['continent','location','date']) 

"""
1. Where did COVID-19 begin? 
"""
df_short = df[ df['total_cases'] > 1000 ] 
countries=df_short['location'].sort_values().unique()
countries=[countries[i] for i in range(len(countries))]
COUNTRY = []
DATE = []
for i, country in enumerate(countries): 
    df_new = df_short[ 
             df_short['location'] == country
    ].sort_values('date').head(1) 
    COUNTRY.append(country)
    DATE.append(df_new.iloc[0]['date'])
country_date = pd.DataFrame()
country_date['Country'] = COUNTRY
country_date['Date'] = DATE
print(country_date\
.sort_values('Date')\
.head(10)\
.reset_index(drop=True))
print(country_date\
.sort_values('Date')\
.tail(10)\
.reset_index(drop=True))

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

