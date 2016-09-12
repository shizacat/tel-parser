#!/usr/bin/python3
# -*- coding: utf-8

# Dependens
# ---------
# Docker: docker run --name tp-postgres -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres
# docker run -it --rm --link tp-postgres:postgres postgres psql -h postgres -U postgres
# apt-get install python3-psycopg2
# SQL
# create user test with password 'test';
# create database tel;
# grant all privileges on database tel to test;
# 

import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime, timezone
import sys, os, re

DB_HOST='localhost'
DB_NAME='tel'
DB_USER='test'
DB_PASS='test'
DB_TABLE='main'

# Не выводить информационные сообщения
C_QUITE=False

# Класс для добавления записей в базу
class cnr:
	# Колличество повторных ключей, и массив их
	valEDK = 0
	arrEDK = []

	def __init__(self, nr):
		self.nr = nr

		try:
			self.conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (DB_NAME, DB_USER, DB_HOST, DB_PASS))
		except Exception as e:
			print("Не могу подключиться к базе. Ошибка: %s" % (e,))
			raise ValueError

		cur = self.conn.cursor()

		try:
			cur.execute("CREATE TABLE IF NOT EXISTS %s (nr varchar(128), d varchar(128), n varchar(128), zp varchar(128), zv varchar(128), a varchar(128), du varchar(128), c varchar(128), dup varchar(128), f varchar(128), bd varchar(128), cur varchar(128), gmt varchar(128), s varchar(128), hd timestamp, hdu_s smallint, hc numeric(15,4), hn varchar(42), cdirection char(1), hnsyf varchar(128), PRIMARY KEY(nr, hd, s))" %(DB_TABLE,) )
			self.conn.commit()
		except Exception as e:
			raise ValueError(e)

		cur.close

	def __del__(self):
		self.conn.close()

	def add(self, value):
		
		attr_reqire = ['d', 'gmt', 'du', 'f', 'cur', 'dup', 'c', 'a', 'zv', 'n', 'zp', 's', 'bd']

		l = list(set(attr_reqire) - set(value.keys()))
		if len(l) != 0:
			raise ValueError("В списке отсутствуют обязательные поля: %s"%(l))

		# Преобразуем дату к формату timestamp без часового пояса
		hd = datetime.strptime( value['d'] + " " + value["gmt"].replace(':','') , "%d.%m.%Y %H:%M:%S %z")
		hd = hd.astimezone(tz=timezone.utc).replace(tzinfo=None)

		# Преобразуем длительность в секунды, которое храниться как целое
		tmp_s = value["du"].split(':')
		if len(tmp_s) == 2:
			hdu_s = int(tmp_s[0])*60 + int(tmp_s[1])
		else:
			hdu_s = 0

		# Преобразуем денежный тип к numeric
		hc = value["c"].replace(',', '.')

		# Нормализуем номер
		hn = re.search('\d+', value["n"])
		if hn:
			hn = hn.group(0)
			if re.search('^8\d+', hn):
				hn = re.sub('^8', '7', hn, 1);
		else:
			hn = ''

		# Определяем направление вызова
		if re.match('^<--', value["n"]):
			cdirection = 'i'
		else:
			cdirection = 'o'

		# hnsyf
		hnsyf = re.search('(?<=<--)[a-zA-Z0-9_-]+(?=:)', value["n"])
		if hnsyf:
			hnsyf = hnsyf.group(0)
		else:
			hnsyf = ''


		cur = self.conn.cursor()
		
		try:
			cur.execute("INSERT INTO %s (d, n, zp, zv, a, du, c, dup, f, bd, cur, gmt, s, hd, hdu_s, hc, nr, hn, cdirection, hnsyf) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')" % (DB_TABLE, value['d'], value['n'], value['zp'], value['zv'], value['a'], value['du'], value['c'], value['dup'], value['f'], value['bd'], value['cur'], value['gmt'], value['s'], hd, hdu_s, hc, self.nr, hn, cdirection, hnsyf))
		except psycopg2.IntegrityError:
			self.valEDK += 1
			self.arrEDK.append( str(hd) +", "+ str(value["s"]))
			self.conn.rollback()
		except Exception as e:
			self.conn.commit()
			raise ValueError("Ошибка в запросе к бд: %s" % (e,))

		self.conn.commit()

		cur.close

# Разбираем xml файл и ложим его в базу		
def parse_xml(f_xml):
	try:
		tree = ET.parse(f_xml)
	except Exception as e:
		print("[!] Ошибка в файле:", f_xml)
		return

	root = tree.getroot()

	for ds in root.findall("./ds"):
		n = ds.attrib["n"]
		
		try:
			db = cnr( n)
		except ValueError as e:
			print("[!]", "[%s]"%(n,), e)
			continue
		
		for i in ds.findall('i'):
			try:
				db.add(i.attrib)
			except ValueError as e:
				print("[!]", "[%s]"%(n,), e)

		if not C_QUITE:
			print("[I] Завершена обработка: %s. Stat: Дубликатов ключей = %s. List: %s" % (n, db.valEDK, db.arrEDK))

if __name__ == "__main__":
	if len(sys.argv) <= 1:
		print("Укажите имя файла или каталог")
		sys.exit(-1)

	f = sys.argv[1]

	if os.path.isdir(f):
		for l in os.listdir(f):
			if not re.match(r'.*\.xml', l) or not os.path.isfile( os.path.join(f,l)):
				continue

			if not C_QUITE:
				print("[I] Обработка файла:", os.path.join(f,l))
			parse_xml( os.path.join(f,l))
		sys.exit(0)

	if os.path.isfile(f):
		parse_xml(f)
		sys.exit(0)

	print("Что то вы мне не то передали")
	sys.exit(-1)






