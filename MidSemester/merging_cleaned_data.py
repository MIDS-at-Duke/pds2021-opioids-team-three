#!/usr/bin/env python
# coding: utf-8

# ### Merging data **by Larissa**
# Here we will merge the following cleaned datasets:
# - prescription from Florida, Kentucky, North Carolina, Georgia
# - death data from all states
# - census data from all states
# 
# ---

# importing a csv with states and state abbreviations to change the states to abbrevations in all datasets

# In[689]:


import pandas as pd
df_states = pd.read_csv('USA_States.csv', encoding = "utf-8")


# Import first dataset containing prescription data

# In[690]:


import pandas as pd
df_prescriptions = pd.read_csv('https://raw.githubusercontent.com/MIDS-at-Duke/pds2021-opioids-team-three/main/20_intermediate/prescriptions?token=AI3GZDZGWG25Z2T5V2RXQGDBRO3WS', index_col=0, encoding='utf-8')
df_prescriptions.head()


# Renaming the column T_DATE to have it homogenous in all data sets

# In[691]:


df_prescriptions = df_prescriptions.rename({'T_DATE': 'Year', 'BUYER_COUNTY': 'County'}, axis = 1)


# Changing the values in County to be capitalized and lowercase to be the same as in the other dataframes:

# In[692]:


df_prescriptions['County'] = df_prescriptions['County'].str.lower()
df_prescriptions['County'] = df_prescriptions['County'].str.capitalize()
df_prescriptions.head()


# Checking missing data

# In[693]:


df_prescriptions.isna().sum()


# Checking how many states this data frame has

# In[694]:


df_prescriptions['State'].unique()


# Adding a state abbreviation column by merging

# In[695]:


df_prescriptions_state = pd.merge(df_prescriptions, df_states, on = 'State', how = 'left' , indicator = True)
df_prescriptions_state.sample(20)


# Checking merge

# In[696]:


df_prescriptions_state[df_prescriptions_state._merge != "both"]


# In[697]:


df_prescriptions_state = df_prescriptions_state.drop('_merge', axis = 1)


# In[698]:


df_prescriptions_state.head()


# Importing thr second dataset containing the death data

# In[699]:


df_deaths = pd.read_csv('https://raw.githubusercontent.com/MIDS-at-Duke/pds2021-opioids-team-three/main/20_intermediate_files/deaths.csv?token=AI3GZD6NJFVWRHJMICW5FSTBRO4Z2')
df_deaths['State'] = df_deaths['State'].str.replace(' ','')
df_deaths.head()


# Displaying all states in the data

# In[700]:


df_deaths['State'].unique()


# Changing the datatype of Year from float to int to have the same data format for Year in all dataframes

# In[701]:


df_deaths["Year"]= df_deaths["Year"].astype('int64')
df_deaths.head()


# In[702]:


df_deaths_state = pd.merge(df_deaths, df_states, left_on = 'State', right_on = 'State Abbr', how = 'left', indicator= True)
df_deaths_state.sample(20)


# Checking merge

# In[703]:


df_deaths_state[df_deaths_state._merge != "both"]


# In[704]:


df_deaths_state =df_deaths_state.drop('State_x', axis = 1)
df_deaths_state =df_deaths_state.drop('_merge', axis = 1)
df_deaths_state = df_deaths_state.rename(columns={'State_y': 'State'})


# In[705]:


df_deaths_state.head()


# Checking missing data

# In[706]:


df_deaths_state.isna().sum()


# Importing the third datasset containing census data

# In[707]:


df_census = pd.read_csv('https://raw.githubusercontent.com/MIDS-at-Duke/pds2021-opioids-team-three/main/MidSemester/20_intermediates/CountyPopulations.csv?token=AI3GZD36R6FVBM277DEVPB3BRRDSC', index_col= 0)
df_census.head()


# We are dropping Bedfort City county because it changed from being an independant city to being a town within a county in 2011.

# In[739]:


df_cenus = df_census.drop(df_census[df_census.County == 'Beldford city'].index)


# In[708]:


df_census['State'].unique()


# In[709]:


df_census_state = pd.merge(df_census, df_states, on = 'State', how = 'left' , indicator = True)
df_census_state.head()


# Checking missing data

# In[710]:


df_census_state.isna().sum()


# In[734]:


df_census[df_census.isnull().any(axis = 1)]


# Checking merge

# In[711]:


df_census_state[df_census_state._merge != 'both']


# In[712]:


df_census_state =df_census_state.drop('_merge', axis = 1)


# Print original dataframe row numbers 

# In[713]:


print(df_prescriptions.shape[0],  'Number of rows for Prescription data')
print(df_deaths.shape[0], 'Number of rows for death data')
print(df_census.shape[0], 'Number of rows for census data')


# Print merged dataframe sums

# In[714]:


print(df_prescriptions_state.shape[0],  'Number of rows for Prescription data')
print(df_deaths_state.shape[0], 'Number of rows for death data')
print(df_census_state.shape[0], 'Number of rows for census data')


# ### Next step merging
# We want to merged dataframes in the end, hence we merge:
# - prescriptiption data with census
# - death data with census
# 
# ---

# First merge of prescription and census

# In[715]:


df_presc_cens = pd.merge(df_prescriptions, df_census, on=['County', 'Year', 'State'],
                                  how='left', validate = 'one_to_one', indicator= 'Merge_value')


# Looking at the merged data, and the missing data.

# In[716]:


df_presc_cens.head()


# In[717]:


df_presc_cens.isna().sum()


# In[718]:


df_presc_cens.describe()


# In[719]:


df_presc_cens[df_presc_cens.Merge_value != 'left_only']


# In[720]:


df_presc_cens = df_presc_cens.drop('Merge_value', axis = 1)


# In[721]:


df_presc_cens.head()


# ### Merging the death data with census data

# In[722]:


df_death_cens = pd.merge(df_deaths_state, df_census_state, on=['County', 'Year', 'State'],
                                  how='inner', validate = 'one_to_one', indicator= 'Merge_Check')


# In[723]:


df_death_cens.head()


# In[724]:


df_death_cens.isna().sum()


# In[725]:


df_death_cens[df_death_cens.Merge_Check != 'both']


# In[726]:


df_death_cens = df_death_cens.drop('Merge_Check', axis = 1)


# In[727]:


print(df_presc_cens.shape[0], 'Number of rows for prescription merged with census data')
print(df_death_cens.shape[0], 'Number of rows for death merged with census data')


# In[728]:


df_death_cens.head()


# Creating a csv file in case we need that

# In[729]:


pd.DataFrame.to_csv(df_presc_cens, 'merged_presc_cens_data.csv', sep=',', na_rep='.', index=False)


# In[730]:


pd.DataFrame.to_csv(df_death_cens, 'merged_death_cens_data.csv', sep=',', na_rep='.', index=False)

