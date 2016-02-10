
#IT RUNS FOR JOBS WHO HAVE AGREED DATE == TODAY () + 1
#IF ROUTIFIC IS RUN AGAIN, IT AGAIN PICKS ALL THE JOBS BASED ON ABOVE CONDITION AND VENDORS BASED ON FILTER CRITERIA AND REASSIGNS
#IF WE NEED TO RUN THE ALGO ONLY FOR SOME JOBS, THEN WE NEED TO SET SOME FLAG
#IT ASSUMES A VENDOR DB, WHICH IS RPESENTLY ON CENTRAL HUB
#IT ALSO ASSUMES A TRIGGER TO START, WHICH RIGHT NOW IS CELL B2 OF SHEET DASHBOARD OF CENTRAL HUB
#IT STORES THE OPTPUT AT TWO PLACE, 1- ROUTIFIC OUTPUT SHEET ON CENTRAL HUB AND CHAINING MODEL IN ONLINE DB

import requests, json, time, csv, gspread, datetime, sys, traceback, urllib, hmac
from oauth2client.client import SignedJwtAssertionCredentials
from hashlib import sha1

##SETS THE PATH TO THE FILE DELAY_MINIMIZATION.PY. HAS TO BE CHANGED ACCORDINGLY
sys.path.insert(0, '/home/rk/PycharmProjects/fms/DMS')
from delay_minimization import delayMin

#LOGS IN INTO GOOGLE SPREADSHEET
def gspreadlogin(name):
	json_key = json.load(open('API Project-063836b65286.json'))
	scope = ['https://spreadsheets.google.com/feeds']
	credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
	gc = gspread.authorize(credentials)

	sh = gc.open_by_key('1U-93KIijTsQtoIkZ2q8ZCwzawjk3T8UNKCtTgzM8pnI')
	worksheet = sh.worksheet(name)

	return worksheet


#UPDATE THE SPREADSHEET
def updateSpreadsheet(cell_list, list,worksheet):
	for cell,val in zip(cell_list,list):
		cell.value = val
	worksheet.update_cells(cell_list)


##CHECKS IF KEY PRESENT IN DICT OR NOT
def getKey(key, dict):
	if key in dict:
		return True
	return False


##GETS VENDORS FROM SPREADSHEET. CAN BE REPLACED BY SERVER. IT ASSUMES THAT VENDOR LIST GIVEN HAS ALREADY BEEN FILTERED
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


##UPDATES TRACKING MODEL WITH COORDINATES OF LOCATION
def updateLatLng(booking_id,p_lat,p_lng,d_lat,d_lng):
	url = "http://52.24.6.41/api/v1/jobs/"+str(booking_id)+"/tracking"
	headers = {'content-type': 'application/json'}
	payload = {
		"pickup": {
			"coords": {
				"lat": p_lat,
				"lng": p_lng
			}
		},
		"delivery": {
			"coords": {
				"lat": d_lat,
				"lng": d_lng
			}
		}
	}

	print url, payload

	try:
		r = requests.post(url, data=json.dumps(payload), headers=headers)
	except:
		print sys.exc_info()[0]
		print traceback.format_exc()
		pass


##CALLS GOOGLE API FOR LATLNG OF PICK AND DROP LOCATIONS
def getLatLng(x):
	if getKey('address_pickup',x):
		if x['address_pickup'] == "":
			pickup = x['pickup_location']
		else:
			pickup = x['address_pickup']
	else:
		pickup = x['pickup_location']

	if getKey('address_delivery',x):
		if x['address_delivery'] == "":
			delivery = x['delivery_location']
		else:
			delivery = x['address_delivery']
	else:
		delivery = x['delivery_location']

	try:
		link1 = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+pickup.replace('\n',' ').replace(' ','%20')
		r = requests.get(link1)
		html = r.json()
		p_lat = html["results"][0]["geometry"]["location"]["lat"]
		p_lng = html["results"][0]["geometry"]["location"]["lng"]
	except:
		try:
			link1 = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+pickup.replace('\n',' ').replace(' ','%20')+", UK"
			r = requests.get(link1)
			html = r.json()
			p_lat = html["results"][0]["geometry"]["location"]["lat"]
			p_lng = html["results"][0]["geometry"]["location"]["lng"]
		except:
			p_lat = ""
			p_lng = ""
			print sys.exc_info()[0]
			print traceback.format_exc()
	try:
		link2 = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+delivery.replace('\n',' ').replace(' ','%20')
		r = requests.get(link2)
		htm2 = r.json()
		d_lat = htm2["results"][0]["geometry"]["location"]["lat"]
		d_lng = htm2["results"][0]["geometry"]["location"]["lng"]
	except:
		try:
			link2 = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+delivery.replace('\n',' ').replace(' ','%20')+", UK"
			r = requests.get(link2)
			htm2 = r.json()
			d_lat = htm2["results"][0]["geometry"]["location"]["lat"]
			d_lng = htm2["results"][0]["geometry"]["location"]["lng"]
		except:
			d_lat=""
			d_lng=""
			print sys.exc_info()[0]
			print traceback.format_exc()

	updateLatLng(x['booking_id'],p_lat,p_lng,d_lat,d_lng)


##GETS THE COMPLETE ONLINE DB	
def get_db():
	headers = {'content-type': 'application/json'}
	url = "http://52.24.6.41/api/v1/jd/all"
	
	while True:
		try:
			db = requests.get(url, headers=headers)
			if db:
				return db.json()['data']
				break
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			pass	
        
        
##GETS JOBS FROM DB
def getJobs(payload):
	db = get_db()

	##THIS SERVICE IS ONLY USED TO CALCULATE LATLNG AS THEY ARE NOT YET STORED IN DB. THIS CAN LATER BE TAKEN OUT AND USED AS A SEPERATE SERVICE. MAYBE AT THE TIME OF BOOKING
	for x in db:
		if not getKey('geo_events',x):
			getLatLng(x)

	##JUST TO RELODE DB WITH UPDATED LATLNG VALUES
	db = get_db()

	##FILTERING OF JOBS TO BE SENT FOR CHAINING
	for x in db:
		if x['agreed_date'] != "":#== str(datetime.date.today() + datetime.timedelta(days=1)):# and x["booking_id"] == 2289:
			volume = 4 if not getKey('volume_total',x) else x['volume_total'] if x['volume_total'] == "" else 4
			pickup_time_from = "8:00" if not getKey('pickup_time_from',x) else datetime.datetime.strptime(x['pickup_time_from'],"%I:%M %p").strftime("%H:%M") if['pickup_time_from'] == "" else "8:00"
			pickup_time_to ="20:00" if not getKey('pickup_time_to',x) else datetime.datetime.strptime(x['pickup_time_to'],"%I:%M %p").strftime("%H:%M") if['pickup_time_to'] == "" else "20:00"
			delivery_date_from = "8:00" if not getKey('delivery_time_from',x) else datetime.datetime.strptime(x['delivery_time_from'],"%I:%M %p").strftime("%H:%M") if['delivery_date_from'] == "" else "8:00"
			delivery_date_to ="20:00" if not getKey('delivery_time_to',x) else datetime.datetime.strptime(x['delivery_time_to'],"%I:%M %p").strftime("%H:%M") if['delivery_date_to'] == "" else "20:00"
			duration = round(x['muscle_time']/2,0)
			type = "1 man" if x['no_of_men'] == 1 else "2 men" if x['no_of_men'] == 2 else ""
			print type
			
			y = delayMin(x)
			z = y.print_something()
			
			payload['visits'].update({
				x['booking_id'] : {
					"load" : volume,
					"pickup" : {
						"location" : {
							"name" : str(x['booking_id'])+"_P",
							"lat" : float(x['geo_events']['pickup']['coords']['lat']),
							"lng" : float(x['geo_events']['pickup']['coords']['lng'])
						},
						"start" : (datetime.datetime.strptime(pickup_time_from,"%H:%M")+ datetime.timedelta(minutes = -1*duration)).strftime("%H:%M"),
						"end" : (datetime.datetime.strptime(pickup_time_to,"%H:%M")+ datetime.timedelta(minutes = -1*duration)).strftime("%H:%M"),
						"duration" : float(duration)
					},
					"dropoff" : {
						"location" : {
							"name" : str(x['booking_id'])+"_D",
							"lat" : float(x['geo_events']['delivery']['coords']['lat']),
							"lng" : float(x['geo_events']['delivery']['coords']['lng'])
						},
						"start" : (datetime.datetime.strptime(delivery_date_from,"%H:%M")+ datetime.timedelta(minutes = -1*duration)).strftime("%H:%M"),
						"end" : (datetime.datetime.strptime(delivery_date_to,"%H:%M")+ datetime.timedelta(minutes = -1*duration)).strftime("%H:%M"),
						"duration" : float(duration)
					},
					"type"    : type.split(",")
				}
			})
			print volume, pickup_time_from, x['booking_id']
	print payload
	return payload,db


##MAKES POST REQUEST TO ROUTIFIC
def runRoutific(payload):
	url = 'https://routific.com/api/v1/pdp-long'
	headers = {'content-type': 'application/json','Authorization': 'bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJfaWQiOiI1NGZlNDczZGFkODc3YTBiMDAwMjA3YTUiLCJpYXQiOjE0MjU5NTA1MjV9.lPvD-AdxImlq9yKGRO0V1uQ1sP6W6f7vUvF-fU1GOyA'}

	while True:
		try:
			r = requests.post(url, data=json.dumps(payload), headers=headers)
			print r.status_code
			print r.text
			if r.status_code == 202:
				return r.json()
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			pass
        

##CHECS STATUS OF EARLIER MADE REQUEST TO ROUTIFIC		
def checkStatus(job_id):        

	status = "processing"
	url = 'https://routific.com/api/jobs/' + str(job_id)

	while status == "processing":
		try:
			time.sleep(10)
			r = requests.get(url)
			status = r.json()['status']
			print status
			if status == "finished":
				return r.json()
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			

##UPDATED CHAINING MODEL			
def update_db(z,n,vendor_name, arrival_time, finish_time, distance, type):
	
	url = "http://52.24.6.41/api/v1/jobs/"+str(z['booking_id'])+"/chaining"
	headers = {'content-type': 'application/json'}
	
	if type == "dropoff":
		type = "delivery"
		
	payload = {
    "chaining_id": vendor_name+"_"+"_".join(str(datetime.datetime.now().date()).split("-")),
    type: {
        "sequence": n,
        "start_time": arrival_time,
        "end_time": finish_time
    },
    "vendor": {
        "name": vendor_name
    },
    "attributes": {
			type+"_distance": distance
		}
}

	r = requests.post(url, data=json.dumps(payload), headers=headers)
	print n,vendor_name, arrival_time, finish_time, distance, type

	
##ROUTIFIC MAIN CALLING FUNCTION	
def mainRoutific(payload, db):
	r = runRoutific(payload)
    	
	try:
		job_id = r['job_id']
		print job_id
	except:
		print r.text
		
	r = checkStatus(job_id)

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
		output = r['output']['solution'][x]
		for dicts in output:
			if a == 1:
				vendor_name = dicts['location_name']
				a+=1
			else:
				final['sequence'].append(n)
				b+=1
				final['vendor_id'].append(x)
				final['vendor_name'].append(vendor_name)
				for key, value in dicts.items():
					final[key].append(value)
				for z in db:
					if str(z['booking_id']) in dicts['location_name']:
						final['job_id'].append(z['booking_id'])
						final['customer_name'].append(z['customer_name'])
						final['phone'].append(z['customer_phone_number'])
						final['email'].append(z['customer_email_id'])
						##NOTE IS YET TO BE MADE, EITHER HERE OR B_1.PY
						final['note'].append(z['sheet_id'])
						if dicts['type'] == 'pickup':
							final['address'].append(z['address_pickup'])
							final['lat'].append(z['geo_events']['pickup']['coords']['lat'])
							final['lng'].append(z['geo_events']['pickup']['coords']['lng'])
						else:
							final['address'].append(z['address_delivery'])
							final['lat'].append(z['geo_events']['delivery']['coords']['lat'])
							final['lng'].append(z['geo_events']['delivery']['coords']['lng'])
						arrival_time = datetime.datetime.strptime(z['agreed_date']+" "+dicts['arrival_time'],"%Y-%m-%d %H:%M") + datetime.timedelta(hours = -1)	
						finish_time = datetime.datetime.strptime(z['agreed_date']+" "+dicts['finish_time'],"%Y-%m-%d %H:%M") + datetime.timedelta(hours = -1)	
						update_db(z,n,vendor_name, arrival_time.strftime("%Y-%m-%dT%H:%M:00.000Z"), finish_time.strftime("%Y-%m-%dT%H:%M:00.000Z"), dicts['distance'], dicts['type'])
				n+=1		

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


# MAIN FUNCTION
def main():
	while True:
		try:
			# LOGIN INTO CENTRAL HUB AND GET DATA FORM DASHBOARD, CAN BE REPLACED BY API REQUEST FROM A DASHBOARD
			# PAYLOAD THAT ROUTIFIC EXPECTS
			worksheet = gspreadlogin("Dashboard")
			payload = {'visits': {}
				, 'fleet': {}
				, 'options': {
					# 'traffic': 'slow',
					'min_visits_per_vehicle': int(worksheet.acell('B3').value),
					'balance': True,
					'polylines': True
				}
					   }
		# IF THE CELL B2 IS 1, THEN IT CALLS ROUTIFICS API
			print worksheet.acell('B2').value 
			if worksheet.acell('B2').value == "1":
				payload = getVendors(payload)
				payload, db = getJobs(payload)
				worksheet.update_acell('B2', "0")
				#mainRoutific(payload,db)
			time.sleep(180)
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			worksheet.update_acell('B2', "2")
			time.sleep(10)

if __name__ == "__main__":
	main()
