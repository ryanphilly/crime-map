
from os import remove
from os.path import exists
from csv import reader
from zipfile import ZipFile
from collections import defaultdict
from typing import Tuple
from requests import Session

DOWNLOAD_URL = lambda state_abr: f'https://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/2019/{state_abr}-2019.zip'
BASE_CSV = lambda state_abr: f'./data/{state_abr}-2019/{state_abr}/'

def download_data(state_abr: str, session: Session) -> None:
  if exists(f'./data/{state_abr}-2019/{state_abr}'):
    return

  response = session.get(DOWNLOAD_URL(state_abr.upper()), stream=True)
  tmp_zip_loc = f'./tmp/{state_abr}-2019.zip'

  with open(tmp_zip_loc, 'wb') as handle:
    for chunk in response.iter_content(chunk_size=512):
      if chunk:  # filter out keep-alive new chunks
        handle.write(chunk)

  with ZipFile(tmp_zip_loc, 'r') as zip_ref:
    zip_ref.extractall('./data')

  remove(tmp_zip_loc)
  

def gather_incidents_and_offences(state_abr: str) -> Tuple[defaultdict, defaultdict]:
  incidents = defaultdict(dict)

  def gather_incidents():
    with open(BASE_CSV(state_abr)+'NIBRS_incident.csv') as incidents_csv:
      csv_reader = reader(incidents_csv, delimiter=',')
      for row in csv_reader:
        incidents[row[2]] = {
          'AGENCY_ID': row[1],
          'NIBRS_MONTH_ID': row[3],
          'INCIDENT_DATE_ID': row[6],
          'INCIDENT_STATUS': row[11],
          'OFFENSES': list()
        }

  def concat_offenses():
    with open(BASE_CSV(state_abr)+'NIBRS_OFFENSE.csv') as offenses_csv:
      csv_reader = reader(offenses_csv, delimiter=',')
      for row in csv_reader:
        incident_id = row[2]
        incidents[incident_id]['OFFENSES'].append({
          'OFFENSE_ID': row[1],
          'OFFENSE_TYPE_ID': row[3],
          'LOCATION_ID': row[5],
        })

  def gather_offense_types():
    type_map = defaultdict(dict)
    with open(BASE_CSV(state_abr)+'NIBRS_OFFENSE_TYPE.csv') as o_type_csv:
      csv_reader = reader(o_type_csv, delimiter=',')
      for row in csv_reader:
        type_map[row[0]] = {
          'OFFENSE_NAME': row[2],
          'CRIME_AGAINST': row[3],
          'OFFENSE_CATEGORY_NAME': row[7],
          'OFFENSE_GROUP': row[8]
        }
    return type_map

  gather_incidents()
  concat_offenses()
  return incidents, gather_offense_types()


  


