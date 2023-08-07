import json
import pyodbc
import requests


# Read JSON data from a file
api_url = "https://api.covid19india.org/state_district_wise.json"

# Fetch JSON data from the API
response = requests.get(api_url)
if response.status_code == 200:
    json_data = response.json()
else:
    print(
        f"Failed to fetch data from the API. Status code: {response.status_code}")
    exit()

# Database connection parameters
server = 'DESKTOP-31RCNP5'
database = 'pythonTables'
# username = 'your_username'
# password = 'your_password'

# Establish database connection
conn_str = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};Integrated Security=SSPI'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Iterate through each state in the JSON data
for state_name, state_info in json_data.items():
    state_code = state_info['statecode']
    district_data = state_info['districtData']

    # Create the SQL insert statement for the state table
    state_insert_query = "INSERT INTO States (StateName, StateCode) VALUES (?, ?)"
    cursor.execute(state_insert_query, state_name, state_code)

    # Iterate through district data and insert into the database
    for district_name, district_info in district_data.items():
        notes = district_info['notes']
        active = district_info['active']
        confirmed = district_info['confirmed']
        migratedother = district_info['migratedother']
        deceased = district_info['deceased']
        recovered = district_info['recovered']
        delta_confirmed = district_info['delta']['confirmed']
        delta_deceased = district_info['delta']['deceased']
        delta_recovered = district_info['delta']['recovered']

        # Create the SQL insert statement for the districts table
        district_insert_query = "INSERT INTO Districts (StateId, DistrictName, Notes, Active, Confirmed, MigratedOther, Deceased, Recovered, DeltaConfirmed, DeltaDeceased, DeltaRecovered) " \
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        cursor.execute(district_insert_query, 1, district_name, notes, active, confirmed, migratedother, deceased, recovered,
                       delta_confirmed, delta_deceased, delta_recovered)


# Commit changes and close connections
conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully.")


if response.status_code == 200:
    json_data = response.json()

    print(json_data)

else:
    print(f"API request failed with status code: {response.status_code}")


# {
#     "State Unassigned": {
#         "districtData": {
#             "Unassigned": {
#                 "notes": "",
#                 "active": 0,
#                 "confirmed": 0,
#                 "migratedother": 0,
#                 "deceased": 0,
#                 "recovered": 0,
#                 "delta": {
#                     "confirmed": 0,
#                     "deceased": 0,
#                     "recovered": 0
#                 }
#             }
#         },
#         "statecode": "UN"
#     },
#     "Andaman and Nicobar Islands": {
#         "districtData": {
#             "Nicobars": {
#                 "notes": "District-wise numbers are out-dated as cumulative counts for each district are not reported in bulletin",
#                 "active": 0,
#                 "confirmed": 0,
#                 "migratedother": 0,
#                 "deceased": 0,
#                 "recovered": 0,
#                 "delta": {
#                     "confirmed": 0,
#                     "deceased": 0,
#                     "recovered": 0
#                 }
#             },
#             "North and Middle Andaman": {
#                 "notes": "District-wise numbers are out-dated as cumulative counts for each district are not reported in bulletin",
#                 "active": 0,
#                 "confirmed": 1,
#                 "migratedother": 0,
#                 "deceased": 0,
#                 "recovered": 1,
#                 "delta": {
#                     "confirmed": 0,
#                     "deceased": 0,
#                     "recovered": 0
#                 }
#             },
#             "South Andaman": {
#                 "notes": "District-wise numbers are out-dated as cumulative counts for each district are not reported in bulletin",
#                 "active": 19,
#                 "confirmed": 51,
#                 "migratedother": 0,
#                 "deceased": 0,
#                 "recovered": 32,
#                 "delta": {
#                     "confirmed": 0,
#                     "deceased": 0,
#                     "recovered": 0
#                 }
#             },
#             "Unknown": {
#                 "notes": "",
#                 "active": -13,
#                 "confirmed": 7496,
#                 "migratedother": 0,
#                 "deceased": 129,
#                 "recovered": 7380,
#                 "delta": {
#                     "confirmed": 0,
#                     "deceased": 0,
#                     "recovered": 0
#                 }
#             }
#         },
#         "statecode": "AN"
#     },
#     "Andhra Pradesh": {
#         "districtData": {
#             "Foreign Evacuees": {
#                 "notes": "",
#                 "active": 0,
#                 "confirmed": 434,
#                 "migratedother": 0,
#                 "deceased": 0,
#                 "recovered": 434,
#                 "delta": {
#                     "confirmed": 0,
#                     "deceased": 0,
#                     "recovered": 0
#                 }
#             },
#             "Anantapur": {
#                 "notes": "",
#                 "active": 247,
#                 "confirmed": 156673,
#                 "migratedother": 0,
#                 "deceased": 1089,
#                 "recovered": 155337,
#                 "delta": {
#                     "confirmed": 51,
#                     "deceased": 1,
#                     "recovered": 45
#                 }
#             },
#             "Chittoor": {
#                 "notes": "",
#                 "active": 2778,
#                 "confirmed": 234198,
#                 "migratedother": 0,
#                 "deceased": 1783,
#                 "recovered": 229637,
#                 "delta": {
#                     "confirmed": 175,
#                     "deceased": 5,
#                     "recovered": 360
#                 }
#             },
#             "East Godavari": {
#                 "notes": "",
#                 "active": 3065,
#                 "confirmed": 281384,
#                 "migratedother": 0,
#                 "deceased": 1232,
#                 "recovered": 277087,
#                 "delta": {
#                     "confirmed": 385,
#                     "deceased": 1,
#                     "recovered": 502
#                 }
#             },
#             "Guntur": {
#                 "notes": "",
#                 "active": 1605,
#                 "confirmed": 169978,
#                 "migratedother": 0,
#                 "deceased": 1163,
#                 "recovered": 167210,
#                 "delta": {
#                     "confirmed": 222,
#                     "deceased": 2,
#                     "recovered": 211
#                 }
#             },
#             "Krishna": {
#                 "notes": "",
#                 "active": 3111,
#                 "confirmed": 111221,
#                 "migratedother": 0,
#                 "deceased": 1250,
#                 "recovered": 106860,
#                 "delta": {
#                     "confirmed": 148,
#                     "deceased": 3,
#                     "recovered": 187
#                 }
#             },
#             "Kurnool": {
#                 "notes": "",
#                 "active": 216,
#                 "confirmed": 123525,
#                 "migratedother": 0,
#                 "deceased": 844,
#                 "recovered": 122465,
#                 "delta": {
#                     "confirmed": 10,
#                     "deceased": 0,
#                     "recovered": 26
#                 }
#             },
#             "Other State": {
#                 "notes": "",
#                 "active": 0,
#                 "confirmed": 2461,
#                 "migratedother": 0,
#                 "deceased": 0,
#                 "recovered": 2461,
#                 "delta": {
#                     "confirmed": 0,
#                     "deceased": 0,
#                     "recovered": 0
#                 }
#             },
#             "Prakasam": {
#                 "notes": "",
#                 "active": 1470,
#                 "confirmed": 131478,
#                 "migratedother": 0,
#                 "deceased": 1023,
#                 "recovered": 128985,
#                 "delta": {
#                     "confirmed": 98,
#                     "deceased": 3,
#                     "recovered": 188
#                 }
#             },
#             "S.P.S. Nellore": {
#                 "notes": "",
#                 "active": 2591,
#                 "confirmed": 136685,
#                 "migratedother": 0,
#                 "deceased": 973,
#                 "recovered": 133121,
#                 "delta": {
#                     "confirmed": 177,
#                     "deceased": 1,
#                     "recovered": 168
#                 }
#             },
#             "Srikakulam": {
#                 "notes": "",
#                 "active": 557,
#                 "confirmed": 121217,
#                 "migratedother": 0,
#                 "deceased": 769,
#                 "recovered": 119891,
#                 "delta": {
#                     "confirmed": 82,
#                     "deceased": 0,
#                     "recovered": 45
#                 }
#             },
#             "Visakhapatnam": {
#                 "notes": "",
#                 "active": 544,
#                 "confirmed": 153395,
#                 "migratedother": 0,
#                 "deceased": 1089,
#                 "recovered": 151762,
#                 "delta": {
#                     "confirmed": 63,
#                     "deceased": 1,
#                     "recovered": 181
#                 }
#             },
#             "Vizianagaram": {
#                 "notes": "",
#                 "active": 225,
#                 "confirmed": 81707,
#                 "migratedother": 0,
#                 "deceased": 669,
#                 "recovered": 80813,
#                 "delta": {
#                     "confirmed": 21,
#                     "deceased": 0,
#                     "recovered": 16
#                 }
#             },
#             "West Godavari": {
#                 "notes": "",
#                 "active": 1422,
#                 "confirmed": 171354,
#                 "migratedother": 0,
#                 "deceased": 1071,
#                 "recovered": 168861,
#                 "delta": {
#                     "confirmed": 304,
#                     "deceased": 1,
#                     "recovered": 243
#                 }
#             },
#             "Y.S.R. Kadapa": {
#                 "notes": "",
#                 "active": 586,
#                 "confirmed": 111341,
#                 "migratedother": 0,
#                 "deceased": 627,
#                 "recovered": 110128,
#                 "delta": {
#                     "confirmed": 133,
#                     "deceased": 0,
#                     "recovered": 144
#                 }
#             }
#         },
#         "statecode": "AP"
#     },
# }
