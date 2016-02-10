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

def updateSpreadsheet(cell_list, list,worksheet):
	
	for cell,val in zip(cell_list,list):
		cell.value = val
	worksheet.update_cells(cell_list)
	
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
	
def get_lat_lng(x):
	
	if x['address_pickup'] =="":
		pickup = x['pickup_location']
	else:
		pickup = x['address_pickup']
	
	try:
		if x['address_delivery'] =="":
			drop = x['delivery_location']
		else:
			drop = x['address_delivery']
	except:
		drop = x['delivery_location']
		
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
		link2 = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+drop.replace('\n',' ').replace(' ','%20')
		r = requests.get(link2)
		htm2 = r.json()
		d_lat = htm2["results"][0]["geometry"]["location"]["lat"]
		d_lng = htm2["results"][0]["geometry"]["location"]["lng"]
	except:
		try:
			link2 = "https://maps.googleapis.com/maps/api/geocode/json?&key=AIzaSyAXqrEAqON_ez218bsEFtDfKKnVl15nbiY&address="+drop.replace('\n',' ').replace(' ','%20')+", UK"
			r = requests.get(link2)
			htm2 = r.json()
			d_lat = htm2["results"][0]["geometry"]["location"]["lat"]
			d_lng = htm2["results"][0]["geometry"]["location"]["lng"]
		except:
			d_lat=""
			d_lng=""
			print sys.exc_info()[0]
			print traceback.format_exc()	
			
	update_att_latlng(x['booking_id'],p_lat,p_lng,d_lat,d_lng)		

def update_att_latlng(booking_id,p_lat,p_lng,d_lat,d_lng):

	url = "http://52.24.6.41/api/v1/jobs/"+str(booking_id)+"/attributes"
	headers = {'content-type': 'application/json'}
	payload = {'latlng' : {
							'pickup' : {
										'lat' : p_lat,
										'lng' : p_lng
										},
							'delivery' : {
										'lat' : d_lat,
										'lng' : d_lng
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

def get_key(obj, key):
	if key in obj:
		print key
		return key
	return ''
		
def getJobs(payload):
	
	db = get_db()
	
	for x in db:
		try:
			if x['attributes']['latlng']:
				pass
		except:
			get_lat_lng(x)
			
	db = get_db()
	
	for x in db:
		if x["booking_id"] == 2289:
			volume = 4 if get_key(x,'volume_total') == "" else x['volume_total'] if x['volume_total'] == "" else 4
			pickup_time_from = "8:00" if get_key(x,'pickup_time_from') == "" else datetime.datetime.strptime(x['pickup_time_from'],"%I:%M %p").strftime("%H:%M")
			pickup_time_to ="20:00" if get_key(x,'pickup_time_to') == "" else datetime.datetime.strptime(x['pickup_time_to'],"%I:%M %p").strftime("%H:%M")
			delivery_date_from = "8:00" if get_key(x,'delivery_time_from') == "" else datetime.datetime.strptime(x['delivery_time_from'],"%I:%M %p").strftime("%H:%M")
			delivery_date_to ="20:00" if get_key(x,'delivery_time_to') == "" else datetime.datetime.strptime(x['delivery_time_to'],"%I:%M %p").strftime("%H:%M")
			duration = round(x['muscle_time']/2,0)
			type = "" if get_key(x['attributes'],'routific_type') == "" else x['attributes']['routific_type']
			print x['attributes']
			print type
			
			payload['visits'].update({
					x['booking_id'] : {
										"load" : volume,
										"pickup" : {
													"location" : {
																	"name" : str(x['booking_id'])+"_P",
																	"lat" : float(x['attributes']['latlng']['pickup']['lat']),
																	"lng" : float(x['attributes']['latlng']['pickup']['lng'])
																},
													"start" : pickup_time_from, 
													"end" : pickup_time_to,
													"duration" : float(duration)
													},
										"dropoff" : {
													"location" : {
																	"name" : str(x['booking_id'])+"_D",
																	"lat" : float(x['attributes']['latlng']['delivery']['lat']),
																	"lng" : float(x['attributes']['latlng']['delivery']['lng'])
																},
													"start" : delivery_date_from, 
													"end" : delivery_date_to,
													"duration" : float(duration)
													},
										"type"    : type.split(",")
				}
				})
			print volume, pickup_time_from, x['booking_id']
	print payload
	return payload,db

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

def update_db(z,n,x,vendor_name, arrival_time, finish_time, distance, type):
	
	url = "http://52.24.6.41/api/v1/jd/update"
	headers = {'content-type': 'application/json'}
	
	if type == 'pickup':
		payload = {
    "jd": "{\"booking_id\":"+str(z['booking_id'])+",\"chaining_id\" : \""+vendor_name+"_".join(str(datetime.datetime.now().date()).split("-"))+"\",\"chaining_seq_pickup\" : \""+str(n)+"\",\"chaining_pickup_time\":\""+arrival_time+"\",\"chaining_delivery_time\" : \""+finish_time+"\"}"
}
	else:
		payload = {
    "jd": "{\"booking_id\":"+str(z['booking_id'])+",\"chaining_id\" : \""+vendor_name+"_".join(str(datetime.datetime.now().date()).split("-"))+"\",\"chaining_seq_delivery\" : \""+str(n)+"\",\"chaining_pickup_time\":\""+arrival_time+"\",\"chaining_delivery_time\" : \""+finish_time+"\"}"
}

	r = requests.post(url, data=json.dumps(payload), headers=headers)
	print n,x,vendor_name, arrival_time, finish_time, distance, type
	
def runRoutific(payload, db):
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
			sys.exit()
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
				for key, value in dicts.items():
					final[key].append(value)
				for z in db:
					if str(z['booking_id']) in dicts['location_name']:
						final['job_id'].append(z['booking_id'])
						final['customer_name'].append(z['customer_name'])
						final['phone'].append(z['customer_phone_number'])
						final['email'].append(z['customer_email_id'])
						final['note'].append(z['sheet_id'])
						if dicts['type'] == 'pickup':
							final['address'].append(z['address_pickup'])
							final['lat'].append(z['attributes']['latlng']['pickup']['lat'])
							final['lng'].append(z['attributes']['latlng']['pickup']['lng'])
						else:
							final['address'].append(z['address_delivery'])
							final['lat'].append(z['attributes']['latlng']['delivery']['lat'])
							final['lng'].append(z['attributes']['latlng']['delivery']['lng'])
						update_db(z,n,x,vendor_name, dicts['arrival_time'], dicts['finish_time'], dicts['distance'], dicts['type'])
	
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

def main():
	while True:
		try:
			worksheet = gspreadlogin("Dashboard")
			payload = {	 'visits': {}
				,'fleet': {}
				,'options':{
					#'traffic': 'slow',
					'min_visits_per_vehicle':int(worksheet.acell('B3').value),
					'balance': True,
					'polylines': True
				}
				}
			if worksheet.acell('B2').value == "1":
				print "here"
				payload = getVendors(payload)
				payload,db = getJobs(payload)
				#runRoutific(payload, db)
				worksheet.update_acell('B2', "0")
			time.sleep(180)
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			worksheet.update_acell('B2', "2")
			time.sleep(10)
			
if __name__ == "__main__":
	main()