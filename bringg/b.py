import requests, json, time, csv, gspread, datetime, sys, traceback, urllib, hmac
from math import radians, cos, sin, asin, sqrt
from oauth2client.client import SignedJwtAssertionCredentials
from hashlib import sha1

bringg = csv.writer(open('lat_long.csv', 'a'), dialect='excel', lineterminator='\n')

geo_fence = 100
company_id = 8209
timestamp = 1439426115619
access_token = 's-Vrz2vQ6qmAWacw9zZh'
secret_token = 's-WxTbszGqxAfbgRnGft'

def postSlack(msg):

	url = "https://hooks.slack.com/services/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"
	payload = {"text": msg}
	requests.post(url, data=json.dumps(payload))

def haversine(lon1, lat1, lon2, lat2):

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371
    return c * r

def hashedSig(key,raw):

	raw = urllib.urlencode(raw)
	hashed = hmac.new(key, raw, sha1)
	signature = hashed.hexdigest()
	
	return signature, raw
	
def getJobs(worksheet):

	jobs = {}
		
	n=0
	for key in worksheet.row_values(1):
		n+=1
		jobs[key]=[]
		val = []
		for value in worksheet.col_values(n):
			if value is None:
				value = 0
			val.append(value)
		jobs[key] = val[1:]
		
	return jobs
	
def getVendors():

	try:
		raw = {
				'access_token':'s-Vrz2vQ6qmAWacw9zZh',
				'company_id':'8209',
				'timestamp':'1439426115619'
			}
		signature, raw = hashedSig(secret_token, raw)
		
		url = "http://developer-api.bringg.com/partner_api/users?"+raw+"&signature="+signature
		headers = {'content-type': 'application/json'}
		r = requests.get(url, headers=headers)
		
		output = r.json()
		
		for list in output:
			bringg.writerow([list["lat"],list["lng"],list["name"],str(datetime.datetime.now())])
			yield list
	
	except:
		yield None

			
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

def createCustomer(job,task_type):
	
	if job['name_of_person_at_'+task_type] == "":
		name = job['customer_name']
	else:
		name = job['name_of_person_at_'+task_type]
	
	if job['number_of_person_at_'+task_type] == "":
		phone = str(job['customer_phone_number'])
	else:
		phone = str(job['number_of_person_at_'+task_type])
		
	phone = phone[-10:]
	print phone
	
	if job['address_'+task_type] == "":
		address = job['customer_phone_number']
	else:
		address = job['address_'+task_type]
		
	raw = {
			'access_token':'s-Vrz2vQ6qmAWacw9zZh',
			'company_id':'8209',
			'timestamp':'1439426115619',
			'name' : name,
			'address' : address,
			'phone' : phone,
			'email' : job['customer_email_id']
			}
	signature, raw = hashedSig(secret_token, raw)
		
	url = "http://developer-api.bringg.com/partner_api/customers?"+raw+"&signature="+signature
	headers = {'content-type': 'application/json'}
	r = requests.post(url, headers=headers)
	
	output = r.json()
	
	url = "http://developer-api.bringg.com/partner_api/customers/"+str(output['customer']['id'])+"?"+raw+"&signature="+signature
	headers = {'content-type': 'application/json'}
	r = requests.patch(url, headers=headers)
	
	output = r.json()
	print output
	return output['customer']['id']
	
def createTask(job,vendor, type):
	
	if type != "":
		task_type = type
	else:
		if job['attributes']['bringg']['start'] == "pickup":
			task_type = "pickup"
		else:
			task_type = "delivery"

	customer_id = createCustomer(job,task_type)
	
	raw = {
			'access_token':'s-Vrz2vQ6qmAWacw9zZh',
			'company_id':'8209',
			'timestamp':'1439426115619',
			'customer_id' : customer_id,
			'user_id' : vendor['id'],
			'title' : "TVM - "+str(job['booking_id'])+" "+task_type,
			'scheduled_at' : job['chaining_'+task_type+'_time'],
			'note' : job['sheet_id']
			}
	signature, raw = hashedSig(secret_token, raw)
		
	url = "http://developer-api.bringg.com/partner_api/tasks?"+raw+"&signature="+signature
	headers = {'content-type': 'application/json'}
	r = requests.post(url, headers=headers)
	
	output = r.json()
	
	if task_type == "pickup":
		update_att(job['booking_id'],output['task']['id'],"","","","","",job['attributes']['bringg']['start'])
	else:
		update_att(job['booking_id'],job['attributes']['bringg']['pickup']['task_id'],job['attributes']['bringg']['pickup']['arrived'],job['attributes']['bringg']['pickup']['left'],output['task']['id'],"","",job['attributes']['bringg']['start'])

	return output['task']['id']
	
def deleteTask(id):

	raw = {
			'access_token':'s-Vrz2vQ6qmAWacw9zZh',
			'company_id':'8209',
			'timestamp':'1439426115619'
			}
	signature, raw = hashedSig(secret_token, raw)
		
	url = "http://developer-api.bringg.com/partner_api/tasks/"+str(id)+"?"+raw+"&signature="+signature
	headers = {'content-type': 'application/json'}
	r = requests.delete(url, headers=headers)
	
def geoFence(job, vendor, chains, l):

	task_type = job['attributes']['bringg']['start']
	distance = haversine(vendor['lat'],vendor['lng'],job['attributes']['latlng'][task_type]['lat'],job['attributes']['latlng'][task_type]['lng'])*1000
	print distance
	
	if distance > geo_fence and job['attributes']['bringg'][task_type]['arrived'] != "":
	
		departure_time = str(datetime.datetime.now())
		deleteTask(job['attributes']['bringg'][task_type]['task_id'])
		
		print "vendor "+vendor['name']+" completed "+str(job['booking_id'])+" at "+str(datetime.datetime.now())
		#postSlack("vendor "+vendor['name']+" completed "+jobs['Job ID'][l]+" at "+str(datetime.datetime.now()))
		
		if task_type == "pickup":
			update_att(job['booking_id'],job['attributes']['bringg']['pickup']['task_id'],job['attributes']['bringg']['pickup']['arrived'],departure_time,job['attributes']['bringg']['delivery']['task_id'],"","","delivery")
		else:
			update_att(job['booking_id'],job['attributes']['bringg']['pickup']['task_id'],job['attributes']['bringg']['pickup']['arrived'],job['attributes']['bringg']['pickup']['left'],job['attributes']['bringg']['delivery']['task_id'],job['attributes']['bringg']['delivery']['arrived'],departure_time,"completed")

		job_2 = [job_2 for job_2 in chains[vendor['name']] if job_2['chaining_seq_pickup'] == l+1 or job_2['chaining_seq_delivery'] == l+1]
		
		if job_2:
			if job_2[0]['attributes']['bringg']['pickup']['task_id'] == "":
				createTask(job_2[0], vendor, 'pickup')
			else:
				createTask(job_2[0], vendor, 'delivery')
		else:
			print "no more jobs for vendor ", vendor['name']
		
			
	elif distance < geo_fence and job['attributes']['bringg'][task_type]['arrived'] == "":
	
		arrival_time = str(datetime.datetime.now())
		
		print "vendor "+vendor['name']+" arrived at location of "+str(job['booking_id'])+" at "+str(datetime.datetime.now())
		#postSlack("vendor "+vendors['name']+" arrived at "+jobs['Job ID'][l]+" at "+str(datetime.datetime.now()))
				
		if task_type == "pickup":
			update_att(job['booking_id'],job['attributes']['bringg']['pickup']['task_id'],arrival_time,"",job['attributes']['bringg']['delivery']['task_id'],"","","pickup")
		else:
			update_att(job['booking_id'],job['attributes']['bringg']['pickup']['task_id'],job['attributes']['bringg']['pickup']['arrived'],job['attributes']['bringg']['pickup']['left'],job['attributes']['bringg']['delivery']['task_id'],arrival_time,"","delivery")

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

	try:
		if x['address_pickup'] =="":
			pickup = x['pickup_location']
		else:
			pickup = x['address_pickup']
	except:
		pickup = x['pickup_location']
	
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
			
def update_att(booking_id,p_task_id,p_arrived,p_left,d_task_id,d_arrived,d_left,start):

	url = "http://52.24.6.41/api/v1/jobs/"+str(booking_id)+"/attributes"
	headers = {'content-type': 'application/json'}
	payload = {'bringg' : {
							'pickup' : {
										'task_id' : p_task_id,
										'arrived' : p_arrived,
										'left' : p_left
										},
							'delivery' : {
										'task_id' : d_task_id,
										'arrived' : d_arrived,
										'left' : d_left
										},
							'start' : start
	}
	}
	print url, payload
	
	try:
		r = requests.post(url, data=json.dumps(payload), headers=headers)
	except:
		print sys.exc_info()[0]
		print traceback.format_exc()
		pass
		
def get_current_job(vendor,chains):
	
	index_max = 0
	index_max_g = 0
	
	for x in chains[vendor['name']]:
		for k, v in x.iteritems():
			if k == "chaining_seq_pickup" and x['attributes']['bringg']['pickup']['task_id'] != "" and  x['attributes']['bringg']['pickup']['left'] == "":
				index = v
			elif k == "chaining_seq_delivery" and x['attributes']['bringg']['delivery']['task_id'] != "" and x['attributes']['bringg']['delivery']['left'] == "":
				index = v
			else:
				index = index_max
			index_max = max(index, index_max)
		index_max_g = max(x['chaining_seq_delivery'],index_max_g)	
	print "index_max", index_max, vendor['name'], index_max_g
	
	if index_max == index_max_g:
		job = [job for job in chains[vendor['name']] if job['chaining_seq_pickup'] == index_max-1 or job['chaining_seq_delivery'] == index_max-1]
		if job[0]['chaining_seq_pickup'] == index_max-1:
			if job[0]['attributes']['bringg']['pickup']['left'] != "":
				job = [job for job in chains[vendor['name']] if job['chaining_seq_pickup'] == index_max or job['chaining_seq_delivery'] == index_max]
		elif job[0]['chaining_seq_delivery'] == index_max-1:
			if job[0]['attributes']['bringg']['delivery']['left'] != "":
				job = [job for job in chains[vendor['name']] if job['chaining_seq_pickup'] == index_max or job['chaining_seq_delivery'] == index_max]
		
	elif index_max != 0:
		#l = max([x['chaining_seq_'+x['attributes']['bringg']['start']] for x in chains[vendor['name']] if x['attributes']['bringg'][x['attributes']['bringg']['start']]['task_id'] != ""])
		#print l
		job = [job for job in chains[vendor['name']] if job['chaining_seq_pickup'] == index_max-1 or job['chaining_seq_delivery'] == index_max-1]
	
	else:
		job = [job for job in chains[vendor['name']] if job['chaining_seq_pickup'] == 2]
		
		if job:
			task_id = createTask(job[0],vendor,'pickup')
			index = [index for index,a in enumerate(chains[vendor['name']]) if a['chaining_seq_pickup'] == 2]
			chains[vendor['name']][index[0]]['attributes']['bringg']['pickup']['task_id'] = task_id
			
			job = [job for job in chains[vendor['name']] if job['chaining_seq_pickup'] == 1]
			task_id = createTask(job[0],vendor,'pickup')
			index = [index for index,a in enumerate(chains[vendor['name']]) if a['chaining_seq_pickup'] == 1]
			chains[vendor['name']][index[0]]['attributes']['bringg']['pickup']['task_id'] = task_id
		else:
			job = [job for job in chains[vendor['name']] if job['chaining_seq_delivery'] == 2]
			index = [index for index,a in enumerate(chains[vendor['name']]) if a['chaining_seq_delivery'] == 2]
			
			task_id = createTask(job[0],vendor,'pickup')
			chains[vendor['name']][index[0]]['attributes']['bringg']['pickup']['task_id'] = task_id
			task_id = createTask(job[0],vendor,'delivery')
			chains[vendor['name']][index[0]]['attributes']['bringg']['delivery']['task_id'] = task_id
			
		index_max = 2
	print job[0]["booking_id"], vendor['name']
	
	geoFence(job[0],vendor,chains,index_max)
			
	print "---------------------------------"
		
def main ():

	while True:
		try:
			start = time.time()
			db = get_db()
			
			for x in db:
				try:
					if x['attributes']['bringg']:
						pass
				except:
					get_lat_lng(x)
					update_att(x['booking_id'],"","","","","","","pickup")
			
			db = get_db()
			
			chains = {}
			
			vendor_db = getVendors()
			
			for i in vendor_db:
				if i['status'] == "offline":
					print "vendor "+i['name']+" is offline"
					#postSlack("vendor "+i['name']+" is offline")
						
				chains.update({i['name']:[]})			
					
				try:
					ids = [x['booking_id'] for x in db if x['driver_name'] == i['name'] and str(datetime.date.today() + datetime.timedelta(days=0)) == x["pickup_date_from"] and x['attributes']['bringg']['start'] != 'completed'] #pinup date to
					print ids
				except:
					print sys.exc_info()[0]
					print traceback.format_exc()
					pass
							
				for x in db:
					if x['booking_id'] in ids:
						chains[i['name']].append(x)
				
				if ids:
					get_current_job(i,chains)
			
			print "completion time", time.time() - start
			time.sleep(20)
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			#postSlack(traceback.format_exc())
			time.sleep(20)
			pass
			
if __name__ == "__main__":
	main()