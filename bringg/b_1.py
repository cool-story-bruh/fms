## 
## - RUNS FOR THOSE JOBS WHOSE AGREED_DATE = TODAY() AND NOT PICKUP DATE FROM
## - GETS ALL THE JOBS WHOSE CHAINING MODEL IS PRESENT AND ARE NOT FULFILLED, THAT IS THOSE JOBS THAT ARE CHAINED 
## - RUNS EVERYMORNING AT 6AM AND SENDS VENDORS FIRST JOB ON THEIR APP
## - ONCE VENDOR ARRIVE AT A LOCAITON, IT SEND VENDOR THE NEXT JOB ON HIS APP
## - ONCE A VENDOR LEAVES A LOCATION, IT REMOVE THE JOB FROM VENDORS APP
## - UPDATES EVERYTHING ON ONLINE DB AND TRACKING MODEL ACORDINGLY
##

import requests, json, time, csv, gspread, datetime, sys, traceback, urllib, hmac
from math import radians, cos, sin, asin, sqrt
from oauth2client.client import SignedJwtAssertionCredentials
from hashlib import sha1

##WRITES THE LATLNG OF VENDORS IN A CSV
bringg = csv.writer(open('lat_long.csv', 'a'), dialect='excel', lineterminator='\n')

##GEO FENCE RADIUS
geo_fence = 200
company_id = 8209
timestamp = 1439426115619
access_token = 's-Vrz2vQ6qmAWacw9zZh'
secret_token = 's-WxTbszGqxAfbgRnGft'


##LOGING INTO SPREADSHEETS. CENTRAL HUB
def gspreadlogin(name):
    json_key = json.load(open('API Project-063836b65286.json'))
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
    gc = gspread.authorize(credentials)

    sh = gc.open_by_key('1U-93KIijTsQtoIkZ2q8ZCwzawjk3T8UNKCtTgzM8pnI')
    worksheet = sh.worksheet(name)

    return worksheet


##UPDATING SPREADSHEETS COLUMNS
def updateSpreadsheet(cell_list, list, worksheet):
    for cell, val in zip(cell_list, list):
        cell.value = val
    worksheet.update_cells(cell_list)


##POSTING MESSAGE ON SLACK
def postSlack(msg):
    url = "https://hooks.slack.com/services/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"
    payload = {"text": msg}
    requests.post(url, data=json.dumps(payload))


##HAVERSINE FORMULA TO CALCULATE DISTANCE
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r


##HASH SIGNING FOR BRINGG
def hashedSig(key, raw):
    raw = urllib.urlencode(raw)
    hashed = hmac.new(key, raw, sha1)
    signature = hashed.hexdigest()
    return signature, raw


##CHECKS IF KEY PRESENT IN DICT OR NOT
def get_key(key, dict):
    if key in dict:
        return True
    return False


##GETTING ONLINE DB JOBS THAT ALREADY HAVE CHAINING MODEL IN PLACE. HENCE THEY ARE CHAINED, WHETHER SINGLE JOB CHAINS OR ACTUAL CHAINS
def get_db():
    headers = {'content-type': 'application/json'}
    url = "http://52.24.6.41/api/v1/jd/all"

    while True:
        try:
            db = requests.get(url, headers=headers)
            if db:
                y = [x for x in db.json()['data'] if get_key("chain", x) and not get_key("attributes", x['geo_events'])]
                return y
                break
        except:
            print sys.exc_info()[0]
            print traceback.format_exc()
            pass


##GETTING VENDORS ASSIGNED FOR JOBS FROM ONLINE DB AND GETTING THEIR COORDINATES FROM BRINGG			
def getVendors(db):
    vendors = [x['chain']['vendor']['name'] for x in db if
               x['chain']['vendor']['name'] != "" and str(datetime.date.today() + datetime.timedelta(days=0)) == x[
                   "pickup_date_from"]]
    print set(vendors)

    try:
        raw = {
            'access_token': 's-Vrz2vQ6qmAWacw9zZh',
            'company_id': '8209',
            'timestamp': '1439426115619'
        }
        signature, raw = hashedSig(secret_token, raw)

        url = "http://developer-api.bringg.com/partner_api/users?" + raw + "&signature=" + signature
        headers = {'content-type': 'application/json'}
        r = requests.get(url, headers=headers)

        output = r.json()

        for list in output:
            if list['name'] in set(vendors):
                bringg.writerow([list["lat"], list["lng"], list["name"], str(datetime.datetime.now())])
                yield list

    except:
        yield None


##UPDATES THE TRACKING MODEL, ONE KEY VALUE PAIR AT A TIME
def update_model(booking_id,type,value,key):
    url = "http://52.24.6.41/api/v1/jobs/"+str(booking_id)+"/tracking"
    headers = {'content-type': 'application/json'}
    payload = {
        type : {
            key : value
        }
    }
    print url, payload

    try:
        r = requests.post(url, data=json.dumps(payload), headers=headers)
    except:
        print sys.exc_info()[0]
        print traceback.format_exc()
        pass

    
##CREATES A CUSTOMER ON BRINGG
def createCustomer(job, task_type):
    if job['name_of_person_at_' + task_type] == "":
        name = job['customer_name']
    else:
        name = job['name_of_person_at_' + task_type]

    if job['number_of_person_at_' + task_type] == "":
        phone = str(job['customer_phone_number'])
    else:
        phone = str(job['number_of_person_at_' + task_type])

    phone = phone[-10:]
    print phone

    if job['address_' + task_type] == "":
        address = job[task_type+'_location']
    else:
        address = job['address_' + task_type]

    raw = {
        'access_token': 's-Vrz2vQ6qmAWacw9zZh',
        'company_id': '8209',
        'timestamp': '1439426115619',
        'name': name.encode('utf-8'),
        'address': address.encode('utf-8'),
        'phone': phone.encode('utf-8'),
        'email': job['customer_email_id'].encode('utf-8')
    }
    signature, raw = hashedSig(secret_token, raw)

    url = "http://developer-api.bringg.com/partner_api/customers?" + raw + "&signature=" + signature
    headers = {'content-type': 'application/json'}
    r = requests.post(url, headers=headers)

    output = r.json()

    url = "http://developer-api.bringg.com/partner_api/customers/" + str(
            output['customer']['id']) + "?" + raw + "&signature=" + signature
    headers = {'content-type': 'application/json'}
    r = requests.patch(url, headers=headers)

    output = r.json()
    print output
    return output['customer']['id']


##CREATES A TASK ON BRINGG, EVERY TASK IS ASSOCIATED WITH A CUSTOMER WHICH NEEDS TO CREATED FIRST
def createTask(job, vendor, type):
    customer_id = createCustomer(job, type)

    raw = {
        'access_token': 's-Vrz2vQ6qmAWacw9zZh',
        'company_id': '8209',
        'timestamp': '1439426115619',
        'customer_id': customer_id,
        'user_id': vendor['id'],
        'title': "TVM - " + str(job['booking_id']) + " " + type,
        'scheduled_at': job['chain'][type]["start_time"],
        'note': job['sheet_id'].encode('utf-8')
    }
    signature, raw = hashedSig(secret_token, raw)

    url = "http://developer-api.bringg.com/partner_api/tasks?" + raw + "&signature=" + signature
    headers = {'content-type': 'application/json'}
    r = requests.post(url, headers=headers)

    output = r.json()

    update_model(job['booking_id'],type, output['task']['id'],'task_id')

    return output['task']['id']

    
##DELEATS A TASK IN BRINGG BASED ON TASK ID
def deleteTask(id):
    raw = {
        'access_token': 's-Vrz2vQ6qmAWacw9zZh',
        'company_id': '8209',
        'timestamp': '1439426115619'
    }
    signature, raw = hashedSig(secret_token, raw)

    url = "http://developer-api.bringg.com/partner_api/tasks/" + str(id) + "?" + raw + "&signature=" + signature
    headers = {'content-type': 'application/json'}
    r = requests.delete(url, headers=headers)

    
##RETURNS THE SEQUNCE NUMBER OF CURRENT ACTIVE TASK
def getIndex(chains):
    index_max, index_max_g,index_p,index_d = 0, 0, 0, 0

    for x in chains:
        if get_key('task_id', x['geo_events']['pickup']) and not get_key('end_time', x['geo_events']['pickup']) and get_key('start_time', x['geo_events']['pickup']):
            index_p = x['chain']['pickup']['sequence']
        elif get_key('task_id', x['geo_events']['delivery']) and not get_key('end_time', x['geo_events']['delivery']) and get_key('start_time', x['geo_events']['delivery']):
            index_d = x['chain']['delivery']['sequence']
                
        index_max_g = max(x['chain']['delivery']['sequence'], index_max_g)
        index_max = max(index_p, index_d, index_max)
        
    if index_max == 0:
        for x in chains:
            if get_key('task_id', x['geo_events']['pickup']) and not get_key('end_time', x['geo_events']['pickup']):
                index_p = x['chain']['pickup']['sequence']
            elif get_key('task_id', x['geo_events']['delivery']) and not get_key('end_time', x['geo_events']['delivery']):
                index_d = x['chain']['delivery']['sequence']
            else:
                index_p = index_max 
            index_max = max(index_p, index_d, index_max)    

    return index_max, index_max_g


##RETURNS A JOB OF A GIVEN INDEX/SEQUENCE IN CHAIN
def getIndexJob(index_max, chains, vendor, type):
    try:
        return [job for job in chains if job['chain'][type]['sequence'] == index_max][0]
    except:
        return None

    
##IDENTIFIES WHAT TASK TYPE IS THE CURRENT JOB FOR A VENDOR    
def taskType(index_max, chains):
    for job in chains:
        if job['chain']['pickup']['sequence'] == index_max:
            return "pickup"
        elif job['chain']['delivery']['sequence'] == index_max:
            return "delivery"
    ##THIS WILL PASS DELIVERY SO THAT GETiNDEXjOB RETURNS NULL LIST AND DOES NOT ERRORS OUT
    return "delivery"


##GEO FENCE LOGIC
def geoFence(index_max, job, chains, vendor, type):
    print type
    distance = haversine(vendor['lat'],vendor['lng'],float(job['geo_events'][type]['coords']['lat']),float(job['geo_events'][type]['coords']['lng']))*1000
    print distance
    
    if distance > geo_fence and get_key('start_time',job['geo_events'][type]):
        end_time = str(datetime.datetime.now())
        deleteTask(job['geo_events'][type]['task_id'])

        print "vendor "+vendor['name']+" completed "+str(job['booking_id'])+" at "+str(datetime.datetime.now())
        #postSlack("vendor "+vendor['name']+" completed "+jobs['Job ID'][l]+" at "+str(datetime.datetime.now()))

        update_model(job['booking_id'],type, end_time,'end_time')
        
        ##MARKS A JOB AS COMPLETED
        if type == "delivery":
            update_model(job['booking_id'],"attributes","Y","fulfilled")

    elif distance < geo_fence and not get_key('start_time',job['geo_events'][type]):
        start_time = str(datetime.datetime.now())
        print "vendor "+vendor['name']+" arrived at location of "+str(job['booking_id'])+" at "+str(datetime.datetime.now())
        #postSlack("vendor "+vendors['name']+" arrived at "+jobs['Job ID'][l]+" at "+str(datetime.datetime.now()))
        update_model(job['booking_id'],type, start_time,'start_time')
        
        type_1 = taskType(index_max+1, chains)
        job_1 = getIndexJob(index_max+1, chains, vendor, type_1)

        if job_1:
            createTask(job_1, vendor, type_1)
        else:
            print "no more jobs for vendor ", vendor['name']

            
##GETS THE CURRENT JOB THAT IS IN PROGRESS
def get_current_job(vendor, chains):
    index_max, index_max_g = getIndex(chains)
    print "index_max", index_max, vendor['name'], index_max_g

    if index_max != 0:
        type = taskType(index_max, chains)
        job = getIndexJob(index_max, chains, vendor, type)
        geoFence(index_max, job, chains, vendor, type)
    else:
        index_max = 1
        type = 'pickup'
        job = getIndexJob(index_max, chains, vendor, type)
        task_id = createTask(job, vendor, type)

    print job["booking_id"], vendor['name']
    print "---------------------------------"


##MAIN FUNCTION
def main():
    while True:
        if datetime.datetime.now().hour >= 6:
            try:
                start = time.time()
                db = get_db()
                chains = {}
                vendor_db = getVendors(db)

                for i in vendor_db:
                    if i['status'] == "offline":
                        print "vendor " + i['name'] + " is offline"
                    # postSlack("vendor "+i['name']+" is offline")

                    chains.update({i['name']: []})
                    ids = [x['booking_id'] for x in db if x['chain']['vendor']['name'] == i['name'] and str(
                            datetime.date.today() + datetime.timedelta(days=0)) == x["agreed_date"]]

                    for x in db:
                        if x['booking_id'] in ids:
                            chains[i['name']].append(x)

                    if ids:
                        get_current_job(i, chains[i['name']])

                print "completion time", time.time() - start
                time.sleep(20)
            except:
                print sys.exc_info()[0]
                print traceback.format_exc()
                # postSlack(traceback.format_exc())
                time.sleep(20)
                pass
        else:
            time.sleep(300)


if __name__ == "__main__":
    main()
