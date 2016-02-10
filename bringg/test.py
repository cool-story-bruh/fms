import requests, json, time, csv, gspread, datetime, traceback, urllib, hmac, sys
sys.path.insert(0, '/home/rk/PycharmProjects/fms/DMS')
from delay_minimization import time_window

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
		
def main():
	db = get_db()
	for x in db:
		y = time_window(x)
		x = y.print_something()
		
if __name__ == "__main__":
	main()
		