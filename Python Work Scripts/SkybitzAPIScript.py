import requests
import xml.etree.ElementTree as tree
import smtplib
from email.message import EmailMessage
import sys
import pyodbc

def errorSend(code):
	errorMsg = 'There was an error during the extraction of the API from Skybitz - Error code: ' + code + ' - Check Skybitz documentation for more information'
	emailAddress = 'XX'
	emailPassword = 'XX'

	emailList = ['XX', 'XX', 'XX']

	msg = EmailMessage()
	msg['Subject'] = 'Skybitz API Error'
	msg['From'] = emailAddress
	msg['To'] = emailList
	msg.set_content(errorMsg)

	with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
		smtp.login(emailAddress, emailPassword)
		smtp.send_message(msg)

try:
	response = requests.get('https://xml.skybitz.com:9443/QueryPositions?assetid=All&customer=XX&password=XX&version=2.69&sortby=1', timeout = 300)
	r = response.text
	rXML = r[r.find('<skybitz>'):]
	domXML = tree.ElementTree(tree.fromstring(rXML))
	errorCode = domXML.find('./error').text
	if errorCode != '0':
		errorSend(errorCode)
except:
	errorSend('The request did not get a response from Skybitz servers')
	sys.exit()

conn = pyodbc.connect('Driver={SQL Server}; Server=XX; Database=XX; Trusted_Connection=Yes;')

cursor = conn.cursor() 

def errorDealer(finder):
	if finder is not None:
		var = finder.text
	else:
		var = None
	return var

div  = domXML.findall('./gls')
for item in div:
	# Extract and Insert data into SkybitzAssetsLocationHist table
	mtsn = errorDealer(item.find('mtsn'))
	assetID = errorDealer(item.find('asset/assetid'))
	assetType = errorDealer(item.find('asset/assettype'))
	messageType = errorDealer(item.find('messagetype'))
	extPower = errorDealer(item.find('extpwr'))
	latitude = errorDealer(item.find('latitude'))
	longitude = errorDealer(item.find('longitude'))
	travelSpeed = errorDealer(item.find('speed'))
	direction = errorDealer(item.find('heading'))
	battery = errorDealer(item.find('battery'))
	time = errorDealer(item.find('time'))
	quality = errorDealer(item.find('quality'))
	geoname = errorDealer(item.find('landmark/geoname'))
	street = errorDealer(item.find('address/street'))
	city = errorDealer(item.find('address/city'))
	state = errorDealer(item.find('address/state'))
	country = errorDealer(item.find('address/country'))
	postal = errorDealer(item.find('address/postal'))
	geoTypeName = errorDealer(item.find('landmark/geotypename'))
	skyfenceStatus = errorDealer(item.find('skyfence/skyfencestatus'))
	idleStatus = errorDealer(item.find('idle/idlestatus'))
	idleDuration = errorDealer(item.find('idle/idleduration'))
	idleGap =  errorDealer(item.find('idle/idlegap'))
	messageReceivedTime = errorDealer(item.find('messagereceivedtime'))
	deviceType = errorDealer(item.find('devicetype'))

	cursor.execute("INSERT INTO SkybitzAssetsLocationHist(mobile_terminal_serial_number, asset_id, asset_type, message_type, using_extPower, latitude, longitude, travel_speed_MPH, asset_movement_direction, battery_status, time_stamp_utc, reporting_quality, geoname, street, city, state, country, postal, geotype_name, skyfence_status, idle_status, idle_duration, idle_gap, message_received_in_system_utc, device_type) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (mtsn, assetID, assetType, messageType, extPower, latitude, longitude, travelSpeed, direction, battery, time, quality, geoname, street, city, state, country, postal, geoTypeName, skyfenceStatus, idleStatus, idleDuration, idleGap, messageReceivedTime, deviceType))
	conn.commit()

	divBinary = item.findall('.//binary')
	for binary in divBinary:
		sensorName = errorDealer(binary.find('inputname'))
		sensorStatus = errorDealer(binary.find('inputstate'))
		sensorEvent = errorDealer(binary.find('event'))
		cursor.execute("SELECT TOP 1 instance_id FROM SkybitzAssetsLocationHist ORDER BY instance_id DESC")
		for row in cursor:
			instanceID = row.instance_id
		cursor.execute("INSERT INTO SkybitzBinary(sensor_name, sensor_status, event, instance_id) VALUES(?, ?, ?, ?)", (sensorName, sensorStatus, sensorEvent, instanceID))
		conn.commit()

	divSerial = item.findall('.//serial')
	for serial in divSerial:
		deviceName = errorDealer(serial.find('serialname'))
		serialData = errorDealer(serial.find('serialdata'))
		event  = errorDealer(serial.find('event'))
		cursor.execute("SELECT TOP 1 instance_id FROM SkybitzAssetsLocationHist ORDER BY instance_id DESC")
		for row in cursor:
			instanceID = row.instance_id
		cursor.execute("INSERT INTO SkybitzSerial(device_name, data_associated, event, instance_id) VALUES(?, ?, ?, ?)", (deviceName, serialData, event, instanceID))
		conn.commit()


#closing connection  
conn.close()  
