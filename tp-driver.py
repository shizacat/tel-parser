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

from io import StringIO

DB_HOST='localhost'
DB_NAME='tel'
DB_USER='test'
DB_PASS='test'
DB_TABLE='main'

# Не выводить информационные сообщения
C_QUITE=False

class bill:
	add_buf = StringIO()

	def __init__(self):
		try:
			self.conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (DB_NAME, DB_USER, DB_HOST, DB_PASS))
		except Exception as e:
			print("Не могу подключиться к базе. Ошибка: %s" % (e,))
			raise ValueError

	def __del__(self):
		self.conn.close()

	def execute(self, sql):
		cur = self.conn.cursor()
		try:
			cur.execute(sql)
		except Exception as e:
			raise ValueError("Ошибка в запросе к бд: %s" % (e,)) 
		
		self.conn.commit()
		cur.close

	def add_detail_commit(self):
		cur = self.conn.cursor()
		self.add_buf.seek(0)
		
		try:
			cur.copy_expert("copy details (bn, an, d, n, zp, zv, a, du, c, dup, f, gmt, s, hd, hdu_s, hc, nr, hn, cdirection, hnsyf) from STDIN" % (), self.add_buf)
		except Exception as e:
			self.conn.commit()
			raise ValueError("Ошибка в запросе к бд: %s" % (e,))

		self.conn.commit()
		cur.close

		self.add_buf.close()
		self.add_buf = StringIO()


	def add_detail(self, bn, an, nr, value):
		attr_reqire = ['d', 'n', 'zv', 'zp', 'gmt', 'du', 'f', 'dup', 'c', 'a', 's' ]

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
			# Заменяем 8 в начале номера на 7
			# if re.search('^8\d+', hn):
			# 	hn = re.sub('^8', '+7', hn, 1);

			# Заменяем 7 в начале номера на +7
			# if re.search('^7\d+', hn):
			# 	hn = re.sub('^7', '+7', hn, 1);

			# Убираем + перед номерами
			if re.search('^\+\d+', hn):
				hn = re.sub('^\+', '', hn, 1);
		else:
			hn = ''

		# Определяем направление вызова
		if re.match('^<--', value["n"]):
			cdirection = 'i'
		else:
			cdirection = 'o'

		# Суфикс поястения из номера сохраняем в отдельном поле
		hnsyf = re.search('(?<=<--)[a-zA-Z0-9_-]+(?=:)', value["n"])
		if hnsyf:
			hnsyf = hnsyf.group(0)
		else:
			hnsyf = ''


		# IO
		# self.add_buf.write( "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (bn, an, value['d'], value['n'], value['zp'], value['zv'], value['a'], value['du'], value['c'], value['dup'], value['f'], value['gmt'], value['s'], hd, hdu_s, hc, nr, hn, cdirection, hnsyf ))

		cur = self.conn.cursor()
		
		try:
			cur.execute("INSERT INTO details (bn, an, d, n, zp, zv, a, du, c, dup, f, gmt, s, hd, hdu_s, hc, nr, hn, cdirection, hnsyf) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')" % (bn, an, value['d'], value['n'], value['zp'], value['zv'], value['a'], value['du'], value['c'], value['dup'], value['f'], value['gmt'], value['s'], hd, hdu_s, hc, nr, hn, cdirection, hnsyf))
		except psycopg2.IntegrityError:
			if not C_QUITE:
				print("Дубликат ключей: %s" % ( value))
			self.conn.rollback()
		except Exception as e:
			self.conn.commit()
			raise ValueError("Ошибка в запросе к бд: %s" % (e,))

		self.conn.commit()

		cur.close


def setup():
	lines = [
	"DROP TABLE IF EXISTS invoices, rrp, pai, upss, details",
	"CREATE TABLE IF NOT EXISTS invoices (bn integer UNIQUE, an varchar(128), sd Date, ed Date, bd Date, pn varchar(256), ha numeric(15,4), hua numeric(15,4), hr numeric(15,4), od varchar(64), PRIMARY KEY(bn, an) )",
	"CREATE TABLE IF NOT EXISTS rrp (bn integer, an varchar(128), cn varchar(128), n varchar(128), mr varchar(256), reg varchar(256), ha numeric(15,4), hua numeric(15,4), hr numeric(15,4),  FOREIGN KEY (bn, an) REFERENCES invoices ON DELETE CASCADE, PRIMARY KEY(bn, an, cn) )",
	"CREATE TABLE IF NOT EXISTS pai (bn integer, an varchar(128), cn varchar(128), pa varchar(128), ha numeric(15,4), hua numeric(15,4), hr numeric(15,4), FOREIGN KEY (bn, an, cn) REFERENCES rrp ON DELETE CASCADE, PRIMARY KEY(bn, an, cn, pa))",
	"CREATE TABLE IF NOT EXISTS upss (bn integer, an varchar(128), pa varchar(128), m varchar(128), ssp_ha numeric(15,4), sso_ha numeric(15,4), sst_ha numeric(15,4), ssn_ha numeric(15,4), ssa_ha numeric(15,4), FOREIGN KEY (bn, an) REFERENCES invoices(bn, an) ON DELETE CASCADE, PRIMARY KEY(bn, an, pa, m) )",
	"CREATE TABLE IF NOT EXISTS details (bn integer, an varchar(128), nr varchar(128), d varchar(128), n varchar(128), zp varchar(128), zv varchar(128), a varchar(128), du varchar(128), c varchar(128), dup varchar(128), f varchar(128), gmt varchar(128), s varchar(128), hd timestamp, hdu_s smallint, hc numeric(15,4), hn varchar(42), cdirection char(1), hnsyf varchar(128), FOREIGN KEY (bn, an) REFERENCES invoices(bn, an) ON DELETE CASCADE)",
	]

	try:
		conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (DB_NAME, DB_USER, DB_HOST, DB_PASS))

		cur = conn.cursor()

		for sql in lines:
			cur.execute(sql)
		
		conn.commit()
		cur.close

	except Exception as e:
		print("Ошибка в запросе к бд: %s" % (e,))
		sys.exit(-1)

# В нужный формат sql
def dtohd(dt):
	return datetime.strptime( dt , "%d.%m.%Y").date()

# к типу numeric
def ctohc(c):
	return c.replace(',', '.')

# Разбираем xml файл и ложим его в базу		
def parse_xml(f_xml):
	try:
		tree = ET.parse(f_xml)
	except Exception as e:
		print("[!] Ошибка в файле:", f_xml)
		return

	root = tree.getroot()

	db = bill()

	tmp_b = root.findall("b")[0].attrib	
	bn = tmp_b["bn"]
	an = tmp_b["an"]

	try:

		# Таблица Invoices
		tmp_c = root.findall("c")[0].attrib
		tmp_urp = root.findall("urp")[0].attrib

		db.execute("INSERT INTO invoices (bn, an, sd, ed, bd, pn, ha, hua, hr, od) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (bn, an, dtohd(tmp_b["sd"]), dtohd(tmp_b["ed"]), dtohd(tmp_b["bd"]), tmp_c["pn"], ctohc(tmp_urp["a"]), ctohc(tmp_urp["ua"]), ctohc(tmp_urp["r"]), tmp_urp["od"] ))

		# Сводная таблица счетов
		for rrp in root.findall("./urp/rrp"):
			cn = rrp.attrib["cn"]
			db.execute("INSERT INTO rrp (bn, an, cn, n, mr, reg, ha, hua, hr) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % ( bn, an, cn, rrp.attrib["n"], rrp.attrib["mr"], rrp.attrib["reg"], ctohc(rrp.attrib["a"]), ctohc(rrp.attrib["ua"]), ctohc(rrp.attrib["r"]) ))
			
			for pai in rrp.findall("pai"):
				db.execute("INSERT INTO pai (bn, an, cn, pa, ha, hua, hr) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % ( bn, an, cn, pai.attrib["pa"], ctohc(pai.attrib["a"]), ctohc(pai.attrib["ua"]), ctohc(pai.attrib["r"]) ))

		# Расход ресурсов по номерам
		for upss in root.findall("./utp/upss"):
			pa = upss.attrib["pa"]

			for ps in upss.findall("ps"):
				m = ps.attrib["m"]		# номер
				tp = ps.attrib["tp"]	# Тарифный план
				ssn_ha = ps.attrib["t"]		# НДС
				ssa_ha = ps.attrib["a"]		# Итого
				ssp_ha = '0'
				sso_ha = '0'
				sst_ha = '0'

				for sc in ps.findall("sc"):
					if sc.attrib["s"].lower() == "Периодические услуги".lower():
						ssp_ha = sc.attrib["a"]
						continue
					if sc.attrib["s"].lower() == "Разовые услуги".lower():
						sso_ha = sc.attrib["a"]
						continue
					if sc.attrib["s"].lower() == "Телефонные услуги".lower():
						sst_ha = sc.attrib["a"]
						continue

				db.execute("INSERT INTO upss (bn, an, pa, m, ssp_ha, sso_ha, sst_ha, ssn_ha, ssa_ha) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (bn, an, pa, m, ctohc(ssp_ha), ctohc(sso_ha), ctohc(sst_ha), ctohc(ssn_ha), ctohc(ssa_ha) ))

		# Детализация звонков по номерам
		for ds in root.findall("./ds"):
			nr = ds.attrib["n"] 		# Сетевой ресурс (номер)
			
			for i in ds.findall('i'):
				db.add_detail(bn, an, nr, i.attrib)
				# db.add_detail_commit()

			if not C_QUITE:
		 		print("[I] Завершена обработка: %s" % (nr))
	
	except Exception as e:
		print("Ошибка в запросе к бд: %s" % (e,))
		db.conn.rollback()
		db.execute("delete from invoices where bn='%s' and an='%s'" % ( bn, an ))

if __name__ == "__main__":

	if len(sys.argv) <= 1:
		print("Укажите имя файла или каталог")
		sys.exit(-1)

	if sys.argv[1].lower() == '-s':
		print("Настраиваю базу")
		setup()
		sys.exit(0)

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






