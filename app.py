from hashlib import new
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import pymysql

def get_connection():
	conn = create_engine('mysql+pymysql://<user>:<password>@<host>/<db>').raw_connection()
	return conn

def getLinkOne():
	data = []
	conn = get_connection()
	cursor = conn.cursor()
	sql = """select * from listlpses where action = 0 order by id asc limit 1;"""
	cursor.execute(sql)
	out = cursor.fetchone()
	conn.commit()
	conn.close()
	data.append({'id':out[0],'link':out[1]})
	return data

def proses():
	link = getLinkOne()
	link_id = str(link[0]['id'])
	link_url = str(link[0]['link'])
	data_new = crawlingUrl(link_url)
	data_old = ""
	c = checkData(link_url)
	try:
		if c :
			updateDataDetilTable(data_new,c[0]['old'],c[0]['link'])
		else:
			insertToDetilTable(data_new,data_old,link_url)
		updateStatusLink(link_id)
	except pymysql.Error as err:
		print("Something went wrong: {}".format(err))
		pass
	return 'Proses Succesfully :)'

def insertToDetilTable(data_new,data_old,data_url):
	conn = get_connection()
	cursor = conn.cursor()
	sql = """insert into detillpses (detil_new,detil_old,link_induk) value('{new}','{old}','{url}') ;""".format(new=data_new,old=data_old,url=data_url)
	cursor.execute(sql)
	conn.commit()
	conn.close()
	print(f"Success insert data detil. new data : ",data_url)

def updateDataDetilTable(data_new,data_old,data_url):
	conn = get_connection()
	cursor = conn.cursor()
	sql = """update detillpses set detil_new = '{new}',detil_old = '{old}' where link_induk = '{url}' ; """.format(new=data_new,old=data_old,url=data_url)
	cursor.execute(sql)
	conn.commit()
	conn.close()
	print(f"Success update data detil link : ",data_url)

def updateStatusLink(ids):
	conn = get_connection()
	cursor = conn.cursor()
	sql = """update listlpses set action = {act} where id = {id};""".format(act=1,id=ids)
	cursor.execute(sql)
	conn.commit()
	conn.close()
	print(f"Success update data id : ",ids)

def checkData(link):
	data = []
	conn = get_connection()
	cursor = conn.cursor()
	sql = """select detil_new,link_induk from detillpses where link_induk = "{url}" ;""".format(url=link)
	cursor.execute(sql)
	out = cursor.fetchone()
	if out:
		conn.commit()
		conn.close()
		data.append({'old':out[0],'link':out[1]})
		return data
	else:
		conn.commit()
		conn.close()
		return list()

def crawlingUrl(url):
	response = requests.get('http://'+url)
	webpage = response.content
	soup = BeautifulSoup(webpage, 'xml')
	content = soup.find_all('table')[0]
	return str(content)

print(proses())