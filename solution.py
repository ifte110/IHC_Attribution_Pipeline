import sqlite3
import pandas as pd
import requests
import json
import re

def connect_to_db(db_path):
    try:
        conn = sqlite3.connect(db_path)
        print("Successfully connected to the database")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def close_db_connection(conn):
    if conn:
        conn.close()
        print("Database connection closed")

def fetch_data(conn, table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        print(f"Fetched {len(df)} records from {table_name}")
        return df
    except sqlite3.Error as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

def build_customer_journeys(session_sources, conversions):
    customer_journeys = []
    for _, conv in conversions.iterrows():
        user_id = conv['user_id']
        conv_id = conv['conv_id']
        conv_time = pd.to_datetime(f"{conv['conv_date']} {conv['conv_time']}")
        user_sessions = session_sources[(session_sources['user_id'] == user_id)].copy()
        user_sessions['event_datetime'] = pd.to_datetime(user_sessions['event_date'] + ' ' + user_sessions['event_time'])
        journey = user_sessions[user_sessions['event_datetime'] <= conv_time].sort_values('event_datetime')
        print(f"created journey for conversion #{_}")

        # Get the total number of sessions in this journey
        total_sessions = len(journey)
        
        for i, (_, session) in enumerate(journey.iterrows()):
            # Set conversion=1 only for the last entry in the journey
            is_conversion = 1 if i == total_sessions - 1 else 0

            customer_journeys.append({
                'conversion_id': str(conv_id),
                'session_id': str(session['session_id']),
                'timestamp': str(session['event_datetime']),
                'channel_label': str(session['channel_name']),
                'holder_engagement': int(session['holder_engagement']),
                'closer_engagement': int(session['closer_engagement']),
                'conversion': is_conversion,
                'impression_interaction' : int(session['impression_interaction'])
            })
    print(f"Built {len(customer_journeys)} customer journeys")
    return customer_journeys

def read_api_key(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        print(f"Error: API key file not found at {file_path}")
        return None

def save_response_as_json(response_data, file_path):
    try:
        with open(file_path, 'w') as file:
            json.dump(response_data, file, indent=4)
        print(f"API response saved to {file_path}")
    except IOError as e:
        print(f"Error saving API response: {e}")

def send_to_api(customer_journeys, api_url, api_key):
    headers = {'Content-Type': 'application/json',  'x-api-key': api_key}
    chunk_size = 100  # Limit for a single API request
    responses = []
    for i in range(0, len(customer_journeys), chunk_size):
        chunk = customer_journeys[i:i + chunk_size]
        payload = { 'customer_journeys': chunk }
        try:
            response = requests.post(api_url, data=json.dumps(payload), headers=headers)
            print(f"Response body: {response.text}")
            response.raise_for_status()
            print(f"Sent batch {i // chunk_size + 1}, received response: {response.status_code}")
            responses.append(response.json())
        except requests.exceptions.RequestException as e:
            print(f"Error sending data to API: {e}")
    save_response_as_json(responses, 'api_responses.json')
    return responses


def run_sql_file(conn, file_path):
    try:
        with open(file_path, 'r') as file:
            sql_script = file.read()
        
        # Remove Python-style comments
        sql_script = re.sub(r'#.*', '', sql_script)

        # Remove docstrings (triple-quoted multiline comments)
        sql_script = re.sub(r'""".*?"""', '', sql_script, flags=re.DOTALL)

        conn.executescript(sql_script)
        conn.commit()
        print(f"Executed SQL script from {file_path}.")
    except (sqlite3.Error, FileNotFoundError) as e:
        print(f"Error running SQL script: {e}")



def insert_ihc_data(conn, ihc_data):
    try:
        query = """
        INSERT OR REPLACE INTO attribution_customer_journey (conv_id, session_id, ihc)
        VALUES (?, ?, ?);
        """
        conn.executemany(query, ihc_data)
        conn.commit()
        print(f"Inserted {len(ihc_data)} rows into attribution_customer_journey.")
    except sqlite3.Error as e:
        print(f"Error inserting data: {e}")


def extract_ihc_data(api_responses):
    ihc_data = []
    
    # Iterate over each response in the API responses
    for response in api_responses:
        # Check if the response is successful (statusCode = 200)
        if response.get('statusCode') == 200:
            # Iterate over the list of journeys (value)
            for journey in response.get('value', []):
                conv_id = journey.get('conversion_id')
                session_id = journey.get('session_id')
                ihc = journey.get('ihc', 0.0)  # Default to 0.0 if missing
                ihc_data.append((conv_id, session_id, ihc))

    print(f"Extracted {len(ihc_data)} IHC data points.")
    return ihc_data


def populate_channel_reporting(conn):
    try:
        query = """
        INSERT OR REPLACE INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
        SELECT 
            ss.channel_name,
            ss.event_date AS date,
            IFNULL(SUM(sc.cost), 0) AS cost,
            IFNULL(SUM(acj.ihc), 0) AS ihc,
            IFNULL(SUM(acj.ihc * c.revenue), 0) AS ihc_revenue
        FROM session_sources ss
        LEFT JOIN session_costs sc ON ss.session_id = sc.session_id
        LEFT JOIN attribution_customer_journey acj ON ss.session_id = acj.session_id
        LEFT JOIN conversions c ON acj.conv_id = c.conv_id
        GROUP BY ss.channel_name, ss.event_date
        HAVING SUM(acj.ihc) <= 1.0;
        """
        conn.execute(query)
        conn.commit()
        print("Channel reporting table successfully populated.")
    except sqlite3.Error as e:
        print(f"Error calculating channel reporting: {e}")


def export_channel_reporting_to_csv(conn, output_file):
    try:
        query = """
        SELECT *, 
               CASE WHEN ihc > 0 THEN cost / ihc ELSE 0 END AS CPO,
               CASE WHEN cost > 0 THEN ihc_revenue / cost ELSE 0 END AS ROAS
        FROM channel_reporting;
        """
        df = pd.read_sql_query(query, conn)
        df.to_csv(output_file, index=False)
        print(f"Exported channel reporting to {output_file}")
    except sqlite3.Error as e:
        print(f"Error exporting channel reporting: {e}")


# Main Implementation

db_path = 'challenge.db'
conn = connect_to_db(db_path)


try:
    session_sources = fetch_data(conn, 'session_sources')
    conversions = fetch_data(conn, 'conversions')
    session_costs = fetch_data(conn, 'session_costs')

    customer_journeys = build_customer_journeys(session_sources, conversions)

    
    api_key = read_api_key('api_key.txt')
    conv_type_id = 'ihc_new'
    api_url = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}"

    sql_file_path = 'challenge_db_create.sql'
    run_sql_file(conn, sql_file_path)

    if api_key:
        api_responses = send_to_api(customer_journeys, api_url, api_key)
        ihc_data = extract_ihc_data(api_responses)
        insert_ihc_data(conn, ihc_data)
        populate_channel_reporting(conn)
        export_channel_reporting_to_csv(conn, 'channel_reporting.csv')
        print("Pipeline complete!")

finally:
    close_db_connection(conn)

