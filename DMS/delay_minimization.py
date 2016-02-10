import requests, json, time, csv, gspread, datetime, sys, traceback, urllib, hmac
from oauth2client.client import SignedJwtAssertionCredentials
from hashlib import sha1

class delayMin():
	
	def __init__(self, job):
		self.job = job
	
	def print_something(self):        
		print self.job['booking_id']