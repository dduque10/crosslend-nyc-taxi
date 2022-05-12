import sys
import pandas as pd
import geopandas as gpd
import sqlite3
from shapely.geometry import Point

NYC_SHAPEFILE = 'taxi_zones/taxi_zones.shp'
HISTORY_START = '2009-11'
nyc_gpd = gpd.read_file(NYC_SHAPEFILE).to_crs(epsg=4326)
sql_con = sqlite3.connect('taxi_destinations.db')


def get_nyc_zone_borough(long: float, lat: float) -> tuple:
    point = Point(long, lat)
    row_contains_point = nyc_gpd['geometry'].contains(point)
    if not row_contains_point.values.any():
        return None, None
    zone = nyc_gpd.loc[row_contains_point, 'zone'].values[0]
    borough = nyc_gpd.loc[row_contains_point, 'borough'].values[0]
    return zone, borough


def main():
    input_file = sys.argv[1]
    k = int(sys.argv[2])

    current_month = input_file.split('/')[-1][:-4]
    cur_month_time_fmt = pd.to_datetime(current_month, format='%Y-%m')
    prev_month = cur_month_time_fmt - pd.DateOffset(months=1)

    df = pd.read_csv(input_file)

    # Add pick up and drop off zones and boroughs
    df[['pick_up_zone', 'pick_up_borough']] = df[['Start_Lon','Start_Lat']].apply(lambda x : get_nyc_zone_borough(*x),
                                                                                  axis=1, result_type='expand')
    df[['drop_off_zone', 'drop_off_borough']] = df[['End_Lon','End_Lat']].apply(lambda x : get_nyc_zone_borough(*x),
                                                                                axis=1, result_type='expand')
    # Drop incomplete data
    df.dropna(inplace=True)

    # Build k top popular destinations by number of passengers by zone
    pas_cnt_df = df.groupby(['pick_up_zone', 'drop_off_zone']).sum()[['Passenger_Count']].reset_index()
    pas_cnt_df['rank'] = pas_cnt_df.sort_values('Passenger_Count', ascending=False).groupby(['pick_up_zone'])\
                             .cumcount()+1
    pop_zone_dest = pas_cnt_df.loc[pas_cnt_df['rank'] <= k, ['pick_up_zone', 'drop_off_zone', 'rank']]\
        .sort_values(['pick_up_zone', 'rank'])

    # Build k top popular destinations by number of rides by borough
    ride_cnt_df = df.groupby(['pick_up_borough', 'drop_off_borough']).count()[['Passenger_Count']].reset_index()
    ride_cnt_df['rank'] = ride_cnt_df.sort_values('Passenger_Count', ascending=False).groupby(['pick_up_borough'])\
        .cumcount()+1
    pop_bor_dest = ride_cnt_df.loc[ride_cnt_df['rank'] < k, ['pick_up_borough', 'drop_off_borough', 'rank']]\
        .sort_values(['pick_up_borough', 'rank'])

    # Save dataframe in a table as current ranking for zones
    pop_zone_dest.to_sql('current_popular_zone_destinations', sql_con, if_exists='replace', index=False)
    # Save dataframe in a table as current ranking for boroughs
    pop_bor_dest.to_sql('current_popular_borough_destinations', sql_con, if_exists='replace', index=False)

    # Create history table
    if current_month == HISTORY_START:
        pop_zone_dest.insert(0, 'month', cur_month_time_fmt)
        pop_zone_dest.to_sql('history_popular_zone_destinations', sql_con, if_exists='replace', index=False)

        pop_bor_dest.insert(0, 'month', cur_month_time_fmt)
        pop_bor_dest.to_sql('history_popular_borough_destinations', sql_con, if_exists='replace', index=False)
    # Or update history table
    else:
        # Make sure not to repeat information
        last_month_zone_dest = pd.read_sql(f"""SELECT pick_up_zone, drop_off_zone, rank 
                                               FROM history_popular_zone_destinations 
                                               WHERE month = '{prev_month}'""", sql_con)
        pop_zone_dest = pd.concat([pop_zone_dest, last_month_zone_dest]).drop_duplicates(keep=False)
        pop_zone_dest.insert(0, 'month', cur_month_time_fmt)
        pop_zone_dest.to_sql('history_popular_zone_destinations', sql_con, if_exists='append', index=False)

        # Make sure not to repeat information
        last_month_bor_dest = pd.read_sql(f"""SELECT pick_up_borough, drop_off_borough, rank 
                                               FROM history_popular_borough_destinations 
                                               WHERE month = '{prev_month}'""", sql_con)
        pop_bor_dest = pd.concat([pop_bor_dest, last_month_bor_dest]).drop_duplicates(keep=False)
        pop_bor_dest.insert(0, 'month', cur_month_time_fmt)
        pop_bor_dest.to_sql('history_popular_borough_destinations', sql_con, if_exists='append', index=False)

    sql_con.close()


if __name__=='__main__':
    main()