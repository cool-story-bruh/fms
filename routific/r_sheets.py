import requests, json, time, csv, gspread, datetime, sys, traceback, urllib, hmac
from oauth2client.client import SignedJwtAssertionCredentials
from hashlib import sha1

def gspreadlogin(name):

	json_key = json.load(open('API Project-063836b65286.json'))
	scope = ['https://spreadsheets.google.com/feeds']
	credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
	gc = gspread.authorize(credentials)
	
	sh = gc.open_by_key('1U-93KIijTsQtoIkZ2q8ZCwzawjk3T8UNKCtTgzM8pnI')
	worksheet = sh.worksheet(name)
	
	return worksheet

def getJobs(payload):
	worksheet = gspreadlogin("Routific")
	#x = int(len(worksheet.col_values(1))) + 1
	#y = len(worksheet.row_values(1))
	lat_p = []
	lng_p = []
	lat_d = []
	lng_d = []
	headers = {'content-type': 'application/json'}
	data = worksheet.get_all_records()
	
	for x in data:
		try:
			url = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+x['pick_code'].replace('\n',' ').replace(' ','%20')+",%20UK"
			r = requests.get(url, headers=headers)
			x['pick_lat'] = r.json()["results"][0]["geometry"]["location"]["lat"]
			x['pick_lng'] = r.json()["results"][0]["geometry"]["location"]["lng"]
		except:
			try:
				url = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+x['pick_code'].replace('\n',' ').replace(' ','%20')
				r = requests.get(url, headers=headers)
				x['pick_lat'] = r.json()["results"][0]["geometry"]["location"]["lat"]
				x['pick_lng'] = r.json()["results"][0]["geometry"]["location"]["lng"]
			except:
				x['pick_lat'] = " "
				x['pick_lng'] = " "
				print sys.exc_info()[0]
				print traceback.format_exc()
				
		try:
			url = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+x['drop_code'].replace('\n',' ').replace(' ','%20')+",%20UK"
			r = requests.get(url, headers=headers)
			x['drop_lat'] = r.json()["results"][0]["geometry"]["location"]["lat"]
			x['drop_lng'] = r.json()["results"][0]["geometry"]["location"]["lng"]
		except:
			try:
				url = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+x['drop_code'].replace('\n',' ').replace(' ','%20')
				r = requests.get(url, headers=headers)
				x['drop_lat'] = r.json()["results"][0]["geometry"]["location"]["lat"]
				x['drop_lng'] = r.json()["results"][0]["geometry"]["location"]["lng"]
			except:
				x['drop_lat'] = " "
				x['drop_lng'] = " "
				print sys.exc_info()[0]
				print traceback.format_exc()
		
		payload['visits'].update({
				x['job_id'] : {
									"load" : x['load'],
									"pickup" : {
												"location" : {
																"name" : x['pick_name'],
																"lat" : float(x['pick_lat']),
																"lng" : float(x['pick_lng'])
															},
												"start" : x['pick_start'], 
												"end" : x['pick_end'],
												"duration" : float(x['pick_duration'])
												},
									"dropoff" : {
												"location" : {
																"name" : x['drop_name'],
																"lat" : float(x['drop_lat']),
																"lng" : float(x['drop_lng'])
															},
												"start" : x['drop_start'], 
												"end" : x['drop_end'],
												"duration" : float(x['drop_duration'])
												},
									"type"    : x['type'].split(",")
			}
			})
		lat_p.append(x['pick_lat'])
		lng_p.append(x['pick_lng'])
		lat_d.append(x['drop_lat'])
		lng_d.append(x['drop_lng'])
			
	updateSpreadsheet(worksheet.range('K2:K100'),lat_p,worksheet)
	updateSpreadsheet(worksheet.range('L2:L100'),lng_p,worksheet)
	updateSpreadsheet(worksheet.range('M2:M100'),lat_d,worksheet)
	updateSpreadsheet(worksheet.range('N2:N100'),lng_d,worksheet)
	print data	
	return payload, data

def getVendors(payload):
	worksheet = gspreadlogin("Vendor DB")
	data = worksheet.get_all_records()
	
	for x in data:
		payload['fleet'].update({
								x['vendor'] :	{
													"start_location" : {
																			"id" : x['id'],
																			"name" : x['name'],
																			"lat" : float(x['lat']),
																			"lng" : float(x['lng'])
																		},
													#"end_location" : {     
													#						"id" : x['id'],
													#						"name" : x['name'],
													#						"lat" : float(x['lat']),
													#						"lng" : float(x['lng'])
													#					},
													"shift_start" : x['shift_start'],
													"shift_end" : x['shift_end'],
													"capacity" : float(x['capacity']),
													"type" : x['type'].split(","),
													"speed" : float(x['speed'])
												}
		
		})
	
	return payload

def runRoutific(payload, jobs):
	url = 'https://routific.com/api/v1/pdp-long'
	headers = {'content-type': 'application/json','Authorization': 'bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJfaWQiOiI1NGZlNDczZGFkODc3YTBiMDAwMjA3YTUiLCJpYXQiOjE0MjU5NTA1MjV9.lPvD-AdxImlq9yKGRO0V1uQ1sP6W6f7vUvF-fU1GOyA'}

	while True:
		try:
			r = requests.post(url, data=json.dumps(payload), headers=headers)
			print r.status_code
			print r.text
			if r.status_code == 202:
				break
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			pass
	
	try:
		job_id = r.json()['job_id']
		print job_id
	except:
		print r.text
	
	status = "processing"
	url = 'https://routific.com/api/jobs/' + job_id
	
	while status == "processing":
		try:
			time.sleep(10)
			r = requests.get(url)
			status = r.json()['status']
			print status
			if status == "finished":
				break
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
	
	worksheet = gspreadlogin("Routific Output")
	final = {}
	final['location_name'],final['arrival_time'],final['type'],final['finish_time'],final['location_id'],final['distance'],final['vendor_id'],final['job_id'], final['sequence'],final['vendor_name'],final['customer_name'],final['phone'],final['email'],final['address'],final['lat'],final['lng'],final['note'] = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []
	b=1
	for x in payload['fleet']:
		print x
		n=1
		a=1
		#final['type'].append("-")
		#final['finish_time'].append("-")
		#final['sequence'].append("-")
		output = {}
		output = r.json()['output']['solution'][x]
		for dicts in output:
			if a == 1:
				vendor_name = dicts['location_name']
				a+=1
			else:
				final['sequence'].append(n)
				n+=1
				b+=1
				final['vendor_id'].append(x)
				final['vendor_name'].append(vendor_name)
				final['job_id'].append(dicts['location_name'])
				for key, value in dicts.items():
					final[key].append(value)
				for z in jobs:
					if z['job_id'] in dicts['location_name']:
						final['customer_name'].append(z['name'])
						final['phone'].append(z['phone'])
						final['email'].append(z['email'])
						final['note'].append(z['note'])
						if dicts['type'] == 'pickup':
							final['address'].append(z['pick_code'])
							final['lat'].append(z['pick_lat'])
							final['lng'].append(z['pick_lng'])
						else:
							final['address'].append(z['drop_code'])
							final['lat'].append(z['drop_lat'])
							final['lng'].append(z['drop_lng'])
						
	
	updateSpreadsheet(worksheet.range('A2:A'+str(b+1)),final['location_name'],worksheet)
	updateSpreadsheet(worksheet.range('D2:D'+str(b+1)),final['arrival_time'],worksheet)
	updateSpreadsheet(worksheet.range('C2:C'+str(b+1)),final['type'],worksheet)
	updateSpreadsheet(worksheet.range('E2:E'+str(b+1)),final['finish_time'],worksheet)
	updateSpreadsheet(worksheet.range('B2:B'+str(b+1)),final['location_id'],worksheet)
	updateSpreadsheet(worksheet.range('F2:F'+str(b+1)),final['distance'],worksheet)
	updateSpreadsheet(worksheet.range('G2:G'+str(b+1)),final['vendor_id'],worksheet)
	updateSpreadsheet(worksheet.range('H2:H'+str(b+1)),final['job_id'],worksheet)
	updateSpreadsheet(worksheet.range('I2:I'+str(b+1)),final['sequence'],worksheet)
	updateSpreadsheet(worksheet.range('J2:J'+str(b+1)),final['vendor_name'],worksheet)
	updateSpreadsheet(worksheet.range('K2:K'+str(b+1)),final['customer_name'],worksheet)
	updateSpreadsheet(worksheet.range('L2:L'+str(b+1)),final['phone'],worksheet)
	updateSpreadsheet(worksheet.range('M2:M'+str(b+1)),final['email'],worksheet)
	updateSpreadsheet(worksheet.range('N2:N'+str(b+1)),final['address'],worksheet)
	updateSpreadsheet(worksheet.range('O2:O'+str(b+1)),final['lat'],worksheet)
	updateSpreadsheet(worksheet.range('P2:P'+str(b+1)),final['lng'],worksheet)
	updateSpreadsheet(worksheet.range('Q2:Q'+str(b+1)),final['note'],worksheet)

def updateSpreadsheet(cell_list, list,worksheet):
	
	for cell,val in zip(cell_list,list):
		cell.value = val
	worksheet.update_cells(cell_list)

while True:
	try:
		worksheet = gspreadlogin("Dashboard")
		payload = {	 'visits': {}
			,'fleet': {}
			,'options':{
				#'traffic': 'slow',
				'min_visits_per_vehicle':worksheet.acell('B3').value,
				'balance': True,
				'polylines': True
			}
			}
		if worksheet.acell('B2').value == "1":
			print "here"
			payload, jobs = getJobs(payload)
			payload = getVendors(payload)
			print payload
			runRoutific(payload, jobs)
			worksheet.update_acell('B2', "0")
		time.sleep(180)
	except:
		print sys.exc_info()[0]
		print traceback.format_exc()
		worksheet.update_acell('B2', "2")
		time.sleep(10)