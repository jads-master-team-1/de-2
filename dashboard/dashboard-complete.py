########## PREAMBLE ##########

# libraries needed for data import
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import pandas as pd
import pandas_gbq

# libraries needed for data processing, plotting and dashboard
import streamlit as st
import matplotlib.pyplot as plt

# define dataset credentials and names
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
gbq_project_id = 'de2021-assignment2'

with open('credentials.json') as f:
    data = json.load(f)
cred_json = pd.json_normalize(data)

credentials = service_account.Credentials.from_service_account_file(
    'credentials.json', scopes=["https://www.googleapis.com/auth/cloud-platform"],)


########## IMPORT DATASETS ##########

# queries for Google bigquery
querydict = {
    "sql_medals_table" : """ SELECT * FROM `de2021-assignment2.assignment2.medals_table` """,
    "sql_overview_athletes_disc_per_country" : """ SELECT * FROM `de2021-assignment2.assignment2.overview_athletes_disc_per_country` """,
    "sql_prop_coach_athlete_cty" : """ SELECT * FROM `de2021-assignment2.assignment2.prop_coach_athlete_cty` """,
    "sql_prop_female_tech_disc" : """ SELECT * FROM `de2021-assignment2.assignment2.prop_female_tech_disc` """,
    "sql_short_tall_disc" : """ SELECT * FROM `de2021-assignment2.assignment2.short_tall_disc` """,
    "sql_top_disc_per_country" : """ SELECT * FROM `de2021-assignment2.assignment2.top_disc_per_country` """,
    "sql_young_old_disc" : """ SELECT * FROM `de2021-assignment2.assignment2.young_old_disc` """
}

# importing dataframes in cache for performance optimalization
@st.cache(persist=True)
def fetch_data(table_name, gbq_project_id):
    sql = "sql_" + table_name
    df = pandas_gbq.read_gbq(querydict[sql], project_id=gbq_project_id, dialect="standard")
    return df

sql_medals_table = """ SELECT * FROM `de2021-assignment2.assignment2.medals_table` ORDER BY gold_medal_count DESC, silver_medal_count DESC, bronze_medal_count DESC"""
df_medals_table = pandas_gbq.read_gbq(sql_medals_table, project_id=gbq_project_id, dialect="standard")

df_overview_athletes_disc_per_country = fetch_data("overview_athletes_disc_per_country", gbq_project_id)
df_prop_coach_athlete_cty = fetch_data("prop_coach_athlete_cty", gbq_project_id)
df_prop_female_tech_disc = fetch_data("prop_female_tech_disc", gbq_project_id)
df_short_tall_disc = fetch_data("short_tall_disc", gbq_project_id)
df_top_disc_per_country = fetch_data("top_disc_per_country", gbq_project_id)
df_young_old_disc = fetch_data("young_old_disc", gbq_project_id)


########## DASHBOARD ##########

# title & sidebar
st.title("Data on the Tokyo 2020 olympics")
selectbox = st.sidebar.multiselect(
    "What datasets whould you want to inpect?",
    ("Medals stream", "Number of athletes per country", "Coach to athlete ratio",
    "Percentage female officials", "Shortest & tallest athletes", "Number of athletes per sport per country", "Oldest & youngest athletes")
)

# Medals stream
if "Medals stream" in selectbox:
    st.header("Medals stream")

    st.write(df_medals_table)


# Number of athletes per country
if "Number of athletes per country" in selectbox:
    st.header("Number of athletes per country at the Tokyo 2020 olympics")

    # preprocessing tables
    mask_country = df_overview_athletes_disc_per_country['country'] == 'Total'
    mask_discipline = df_overview_athletes_disc_per_country['discipline'] == 'Total'
    df_sports = df_overview_athletes_disc_per_country[mask_country]
    df_country = df_overview_athletes_disc_per_country[~mask_country]
    df_country_total = df_country[mask_discipline]
    df_country_sport = df_country[~mask_discipline]
    df_country_total = df_country_total.sort_values(by='no_athletes', ascending=False).drop('discipline', axis=1).reset_index(drop=True) # order on no. athletes & drop unused column
    df_country_sport = df_country_sport.sort_values(by='no_athletes', ascending=False).reset_index(drop=True)                            # order on no. athletes
    df_sports = df_sports.sort_values(by='no_athletes', ascending=False).drop('country', axis=1).reset_index(drop=True)                  # order on no. athletes & drop unused column

    # interface and plots
    no = st.slider('Number of countries to show:', 1, 206)
    st.header("Number of total athletes of the top {no} countries".format(no=no))
    st.write(df_country_total[0:no])

    x = list(df_country_total['no_athletes'][0:no])
    labels = list(df_country_total['country'][0:no])
    fig = plt.figure(figsize=(10,10))
    plt.pie(x=x, labels=labels, startangle=90, counterclock=False);

    st.header("Number of athletes of the top {no} countries".format(no=no))
    st.pyplot(fig)


# Coach to athlete ratio
if "Coach to athlete ratio" in selectbox:
    st.header("Coach to athlete ratio")

    # preprocessing tables
    # infinite = df_prop_coach_athlete_cty[df_prop_coach_athlete_cty["proportionAthleteToCoach" == "Infinity"]].index
    df_prop_coach_athlete_cty = df_prop_coach_athlete_cty.sort_values("no_athletes", ascending=False).reset_index(drop=True)

    st.write(df_prop_coach_athlete_cty)


# Percentage female officials
if "Percentage female officials" in selectbox:
    st.header("Percentage female officials")

    # preprocessing tables
    df_prop_female_tech_disc = df_prop_female_tech_disc.drop('female_officials_count', axis=1).drop("count_per_discipline", axis=1)

    st.write(df_prop_female_tech_disc)


# Shortest & tallest athletes
if "Shortest & tallest athletes" in selectbox:
    st.header("Shortest & tallest athletes")

    st.write(df_short_tall_disc)

    data = [df_short_tall_disc.iloc[0,1], df_short_tall_disc.iloc[1,1]]
    labels = [df_short_tall_disc.iloc[0,0], df_short_tall_disc.iloc[1,0]]

    fig = plt.figure(figsize=(8,5))
    plt.bar(labels, data)
    st.pyplot(fig)


# Number of athletes per sport per country
if "Number of athletes per sport per country" in selectbox:
    st.header("Biggest athlete teams")

    # preprocessing
    df_top_disc_per_country = df_top_disc_per_country.sort_values(by='no_athletes', ascending=False).reset_index(drop=True)

    # interface and plots
    no = st.slider('Number of teams to show:', 1, 277)
    st.header("Number of athletes in the {no} biggest teams".format(no=no))
    st.write(df_top_disc_per_country[0:no])

    data = df_top_disc_per_country.iloc[0:no,2]
    label_country = df_top_disc_per_country.iloc[0:no,0]
    label_discipline = df_top_disc_per_country.iloc[0:no,1]
    labels = label_discipline + "\n" + label_country
    
    fig = plt.figure(figsize=(16,5))
    plt.bar(labels, data)

    st.pyplot(fig)


# Oldest & youngest athletes
if "Oldest & youngest athletes" in selectbox:
    st.header("Oldest & youngest athletes")

    st.write(df_young_old_disc)