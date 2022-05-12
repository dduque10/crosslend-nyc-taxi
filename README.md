# NYC Taxi Rides 

This pipeline updates the following four tables:
  - **current_popular_zone_destinations:** Contains the most popular destination zones per pick up zone by the 
number of passengers for the latest month.
  - **history_popular_zone_destinations:** Contains the history of the most popular destination zones per pick up zone 
by the number of passengers.
  - **current_popular_borough_destinations:** Contains the most popular destination boroughs per pick up borough by the 
number of rides for the latest month.
  - **history_popular_borough_destinations:** Contains the history of the most popular destination boroughs per pick up 
zone by the number of rides.

This repository contains the sqlite db: `taxi_destinations.db` which was created by running the pipeline for two sample 
files: `2009-11.csv` and `2009-12.csv` and for `k=5` and setting the history start time as `2009-11`

## How to run it

Run the update_popular_destinations script with two arguments
- The CSV file containing the last month data
- The k parameter (top k destinations)

Ex: 
`python3 -m update_popular_destinations 2009-11.csv 5`

## Assumptions
- Only the yellow taxi trip records were used.
- The monthly data is stored as a CSV file with the following name pattern: YYYY-MM.csv with the same schema as in the 
files in https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Workarounds
- Skip rides with pick up or drop off locations that are not in the Shapefile
- Use SQLite as DB engine
- Tested with small samples from DB (each month file with 10000 records)
