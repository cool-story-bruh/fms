import requests, json, time, csv, gspread, datetime, sys, traceback, urllib, hmac
from math import radians, cos, sin, asin, sqrt
from oauth2client.client import SignedJwtAssertionCredentials
from hashlib import sha1

bringg = csv.writer(open('lat_long.csv', 'a'), dialect='excel', lineterminator='\n')

geo_fence = 200
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

def updateSpreadsheet(cell_list, list,worksheet):
	
	for cell,val in zip(cell_list,list):
		cell.value = val
	worksheet.update_cells(cell_list)

def createCustomer(jobs,l):
	raw = {
			'access_token':'s-Vrz2vQ6qmAWacw9zZh',
			'company_id':'8209',
			'timestamp':'1439426115619',
			'name' : jobs['Customer Name'][l],
			'address' : jobs['Address'][l],
			'phone' : jobs['Customer Phone Number'][l],
			'email' : jobs['Email'][l]
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
	
	return output['customer']['id']
	
def createTask(vendors, jobs,l):
	customer_id = createCustomer(jobs,l)
	raw = {
			'access_token':'s-Vrz2vQ6qmAWacw9zZh',
			'company_id':'8209',
			'timestamp':'1439426115619',
			'customer_id' : customer_id,
			'user_id' : vendors['id'],
			'title' : jobs['Title'][l],
			'scheduled_at' : jobs['Schedule Time'][l],
			'note' : jobs['Note'][l].encode('utf8')
			}
	signature, raw = hashedSig(secret_token, raw)
		
	url = "http://developer-api.bringg.com/partner_api/tasks?"+raw+"&signature="+signature
	headers = {'content-type': 'application/json'}
	r = requests.post(url, headers=headers)
	
	output = r.json()
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
	
def geoFence(jobs, vendors, worksheet):

	l = [index for index,seq in enumerate(jobs['sequence in chain']) if index in [index1 for index1,val in enumerate(jobs['Left Location']) if val==0 and index1 in [index2 for index2,val in enumerate(jobs['Vendor']) if val == vendors['name']]]]
	print l
	
	if l:
		l = min(l)
		
		if jobs['Task ID'][l] == "-":
		
			jobs['Task ID'][l] = createTask(vendors, jobs,l)
			
			if jobs['Task ID'][l+1]:
				jobs['Task ID'][l+1] = createTask(vendors, jobs,l+1)
		
			cell_list = worksheet.range('M2:M100')
			updateSpreadsheet(cell_list,jobs['Task ID'],worksheet)
			
		distance = haversine(vendors['lat'],vendors['lng'],jobs['lat'][l],jobs['lng'][l])*1000
		print distance
		
		if distance > geo_fence and jobs['Arrived'][l] == 1:
		
			jobs['Left Location'][l] = 1
			jobs['Departure Time'][l] = str(datetime.datetime.now())
			cell_list = worksheet.range('O2:O100')
			updateSpreadsheet(cell_list,jobs['Left Location'],worksheet)
			cell_list = worksheet.range('Q2:Q100')
			updateSpreadsheet(cell_list,jobs['Departure Time'],worksheet)
			deleteTask(jobs['Task ID'][l])
			#print "vendor "+vendors['name']+" completed "+jobs['Task ID'][l]+" at "+str(datetime.datetime.now())
			postSlack("vendor "+vendors['name']+" completed "+jobs['Job ID'][l]+" at "+str(datetime.datetime.now()))
			try:
				jobs['Task ID'][l+2] = createTask(vendors, jobs,l+2)
				cell_list = worksheet.range('M2:M100')
				updateSpreadsheet(cell_list,jobs['Task ID'],worksheet)
			except:
				pass
				
		elif distance < geo_fence and jobs['Arrived'][l] == 0:
		
			jobs['Arrived'][l] = 1
			jobs['Arrival Time'][l] = str(datetime.datetime.now())
			cell_list = worksheet.range('N2:N100')
			updateSpreadsheet(cell_list,jobs['Arrived'],worksheet)
			cell_list = worksheet.range('P2:P100')
			updateSpreadsheet(cell_list,jobs['Arrival Time'],worksheet)
			#print "vendor "+vendors['name']+" arrived at location of "+jobs['Task ID'][l]+" at "+str(datetime.datetime.now())
			postSlack("vendor "+vendors['name']+" arrived at "+jobs['Job ID'][l]+" at "+str(datetime.datetime.now()))
			
			
def gspreadlogin(name):

	json_key = json.load(open('API Project-063836b65286.json'))
	scope = ['https://spreadsheets.google.com/feeds']
	credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
	gc = gspread.authorize(credentials)
	
	sh = gc.open_by_key('1U-93KIijTsQtoIkZ2q8ZCwzawjk3T8UNKCtTgzM8pnI')
	worksheet = sh.worksheet(name)
	
	return worksheet

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

def update_db(booking_id):

	url = "http://52.24.6.41/api/v1/jd/update"
	headers = {'content-type': 'application/json'}
	payload = {
    "jd": "{\"booking_id\":"+str(booking_id)+",\"attributes\":[{\"chat_transcript_sent\" : \"Y\"},{\"contact_created\" : \"Y\"}]}"
}
	while True:
		try:
			r = requests.post(url, data=json.dumps(payload), headers=headers)
			if r.status_code == 200:
				break
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			pass
			
def main ():

	while True:
		try:
			start = time.time()
			worksheet = gspreadlogin('Dashboard')
			
			if worksheet.acell('B4').value == "1":
			
				vendors = worksheet.col_values(4)
				print vendors
				worksheet = gspreadlogin('Bringg Input')
				
				jobs = getJobs(worksheet)
				jobs['Arrived'],jobs['Left Location'],jobs['sequence in chain'],jobs['lat'],jobs['lng'] = map(int, jobs['Arrived']),map(int, jobs['Left Location']),map(int, jobs['sequence in chain']),map(float, jobs['Lat']),map(float, jobs['Lng'])
				
				for i in getVendors():
					if i['name'] in vendors:
						if i['status'] == "offline":
							print "vendor "+i['name']+" is offline"
							#postSlack("vendor "+i['name']+" is offline")
							time.sleep(30)
							pass
						geoFence(jobs,i,worksheet)
				
			print "time : "+str(time.time() - start)
		except:
			print sys.exc_info()[0]
			print traceback.format_exc()
			#postSlack(traceback.format_exc())
			time.sleep(20)
			pass
			
if __name__ == "__main__":
	main()