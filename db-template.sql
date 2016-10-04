

CREATE TABLE IF NOT EXISTS invoices (bn integer UNIQUE, an varchar(128), sd Date, ed Date, bd Date, pn varchar(256), ha numeric(15,4), hua numeric(15,4), hr numeric(15,4), od varchar(64), PRIMARY KEY(bn, an) );


bn - номер счета
an - номер лицевого счета абонента
sd - ed - период за который выставлен счет
bd - от какого числа счет
pn - Юридическое имя плательщика
ha  - Израсходовано за период
hua - К оплате
hr  - Остаток
od - Возможно!!! Баланс технологисческого ЛС


-- Сводная таблица лицевых счетов
CREATE TABLE IF NOT EXISTS rrp (bn integer, an varchar(128), cn varchar(128), n varchar(128), mr varchar(256), reg varchar(256), ha numeric(15,4), hua numeric(15,4), hr numeric(15,4),  FOREIGN KEY (bn, an) REFERENCES invoices ON DELETE CASCADE, PRIMARY KEY(bn, an, cn) );
bn
an
cn - номер контракта
n - Юридическое лица
mr - МР
reg - регион
ha  - Израсходовано за период
hua - К оплате
hr  - Остаток

-- Лицевые счета к сводной таблице лицевых счетов
CREATE TABLE IF NOT EXISTS pai (bn integer, an varchar(128), cn varchar(128), pa varchar(128), ha numeric(15,4), hua numeric(15,4), hr numeric(15,4), FOREIGN KEY (bn, an, cn) REFERENCES rrp ON DELETE CASCADE, PRIMARY KEY(bn, an, cn, pa));
bn
an
cn
pa - номер лицевого счета
ha  - Израсходовано за период
hua - К оплате
hr  - Остаток

-- Расход ресурсов по номерам
CREATE TABLE IF NOT EXISTS upss (bn integer, an varchar(128), pa varchar(128), m varchar(128), ssp_ha numeric(15,4), sso_ha numeric(15,4), sst_ha numeric(15,4), ssn_ha numeric(15,4), ssa_ha numeric(15,4), FOREIGN KEY (bn, an) REFERENCES invoices(bn, an) ON DELETE CASCADE, PRIMARY KEY(bn, an, pa, m) );
bn
an
pa - номер лицевого счета
m - номер (сетевой ресурс)
ssp_a - Переодические услуги
sso_a - Разовые услуги
sst_a - Телефонные услуги
ssn_a - НДС
ssa_a - Итого


-- Детализация номеров
CREATE TABLE IF NOT EXISTS details (bn integer DEFAULT NULL, an varchar(128) DEFAULT NULL, nr varchar(128), d varchar(128), n varchar(128), zp varchar(128), zv varchar(128), a varchar(128), du varchar(128), c varchar(128), dup varchar(128), f varchar(128), gmt varchar(128), s varchar(128), hd timestamp, hdu_s smallint, hc numeric(15,4), hn varchar(42), cdirection char(1), hnsyf varchar(128), FOREIGN KEY (bn, an) REFERENCES invoices(bn, an) ON DELETE CASCADE, unn bigint DEFAULT NULL REFERENCES one_doc(unn) ON DELETE CASCADE );

nr - сетевой ресурс (номер телефона)
d 
n - номер из файла
zp - зона направления
zv - зона вызова
a 
du 
c 
dup 
f 
bd 
cur 
gmt 
s - сервис (sms i, sms o, Телеф., ...)
hd - дата вызова, без часового пояса
hdu_s - продолжительность, в секундах
hc - стоимость, NUMERIC(15,4)
cdirection - направление вызова (i - входящие, o - исходящие)
hn - нормализованный номер
hnsyf - суфикс из номера. Примеры: Ya_na_svyazi, sms, Vam_Zvonili


-- =======================
-- Для гражданских номеров
-- Режим one
-- =======================

CREATE TABLE IF NOT EXISTS one_doc ( nr varchar(128), sd Date, ed Date, unn BIGSERIAL UNIQUE, PRIMARY KEY (nr, sd, ed))

nr - сетевой ресурс
sd - дата начала
ed - дата окончания