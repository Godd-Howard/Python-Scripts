import requests
import json
from time import gmtime, strftime
from datetime import datetime, timedelta
import sys
from email.message import EmailMessage
import pyodbc

conn = pyodbc.connect('Driver={SQL Server}; Server=XX; Database=XX; Trusted_Connection=Yes;')

cursor = conn.cursor() 

timeCurr = datetime.utcnow().isoformat()
timePast = datetime.utcnow() - timedelta(hours = 2)
nextPage = timePast.isoformat()

vehicleList = []

def errorSend():
	errorMsg = 'There was an error during the extraction of the API from Maven'
	emailAddress = 'XX'
	emailPassword = 'XX'

	emailList = ['XX', 'XX']

	msg = EmailMessage()
	msg['Subject'] = 'Maven API Error'
	msg['From'] = emailAddress
	msg['To'] = emailList
	msg.set_content(errorMsg)

	with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
		smtp.login(emailAddress, emailPassword)
		smtp.send_message(msg)

def checkKey(dictionary, key):
	if key in dictionary:
		return dictionary[key]
	else:
		return None

header = {'apiKey': 'XX'}

while True:
	try:
		response = requests.get('https://integrations.mavenmachines.com/locations/vehicles?startTime=' + nextPage + '&endTime=' + timeCurr, headers = header)
		r = response.json()
	except:
		errorSend()
		sys.exit()
	for i in r['data']:
		vehicle = checkKey(i, 'vehicle')
		eventTimeMod = checkKey(i, 'eventTime')
		eventTime = datetime.fromisoformat(eventTimeMod.replace('Z', ''))
		uploadTimeMod = checkKey(i, 'uploadedTime')
		uploadTime = datetime.fromisoformat(uploadTimeMod.replace('Z', ''))
		latitude = checkKey(i, 'latitude')
		longitude = checkKey(i, 'longitude')
		odometer = checkKey(i, 'odometer')

		if vehicle not in vehicleList:
			cursor.execute("INSERT INTO MavenVehicleHist(vehicle, event_time_utc, upload_time_utc, latitude, longitude, odometer_reading) VALUES(?, ?, ?, ?, ?, ?)", (vehicle, eventTime, uploadTime, latitude, longitude, odometer))
			conn.commit()
			vehicleList.append(vehicle)

	if r['pagination']['hasNextPage'] != True:
		break
	else:
		nextPage = r['pagination']['nextPageStartTime']

vehicleList.clear()
conn.close()