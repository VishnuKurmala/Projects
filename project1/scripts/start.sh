#Shebang Statement
#!/bin/bash

#Sourcing the param file
. /home/saif/project1/env/sqp.prm

#Creating log file name
LOG_DIR=/home/saif/project1/logs
SCRIPT_FILE=`basename $0`
DT=`date '+%Y%m%d_%H:%M:%S'`
LOG_FILENAME=${LOG_DIR}/${SCRIPT_FILE}_${DT}.log

#Checking whether the table created  in MYSQL
o=$(mysql -uroot -pWelcome@123 -e "select exists(select * from information_schema.tables where table_name='tbl_project1_day');")
p=`echo "${o: -1}"`

if [ $p -eq 0 ]
then
#create table in mysql
mysql -uroot -pWelcome@123 -e "
create table if not exists Project1.tbl_project1_day(
custid integer(10),
username varchar(30),
quote_count varchar(30),
ip varchar(30),
entry_time varchar(30),
prp_1 varchar(30),
prp_2 varchar(30),
prp_3 varchar(30),
ms varchar(30),
http_type varchar(30),
purchase_category varchar(30),
total_count varchar(30),
purchase_sub_category varchar(30),
http_info varchar(30),
status_code integer(10),
Date_col date
);"
echo "Table tbl_project1_day created successfully">>${LOG_FILENAME}
else echo "Table already existed">>${LOG_FILENAME}
mysql -uroot -pWelcome@123 -e "truncate table Project1.tbl_project1_day;"
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Getting latest file
cd /home/saif/project1/datasets
file=`ls -Aru | tail -n 1`
d=`echo ${file} | awk '{split($0,a,"[_.]");print a[2]}'`


cd /home/saif/project1/sqoop

#Loading the everyday data into the database
mysql --local-infile=1 -uroot -pWelcome@123 -e "set global local_infile=1;
LOAD DATA LOCAL INFILE '/home/saif/project1/datasets/${file}' INTO TABLE Project1.tbl_project1_day FIELDS TERMINATED BY ',';
update Project1.tbl_project1_day set Date_col=DATE_ADD(CURDATE(),INTERVAL $d DAY) where Date_col is null or Date_col=0000-00-00;"

#Checking whether data loaded to mysql
if [ $? -eq 0 ]
then echo "${file} loaded to table successfully">>${LOG_FILENAME}
else echo "${file} loading failed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Importing data into HDFS
sqoop import --connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=False --username ${USERNAME} --password-file ${PASSWORD_FILE} --query 'select * from Project1.tbl_project1_day where $CONDITIONS'  --delete-target-dir --target-dir ${OP_DIR}/tbl_project1_day  -m 1

#Checking whether data imported to HDFS
if [ $? -eq 0 ]
then echo "Data ingested to HDFS ${OP_DIR}/tbl_project1_day successfully">>${LOG_FILENAME}
else echo "Data lmported failed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#creating managed table in hive
hive -e "create table  Project1.mngtbl (
custid int,
username string,
quote_count string,
ip string,
entry_time string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int,
Date_col date)
row format delimited fields terminated by ',';"

#Checking whether the managed table created  in hive
if [ $? -eq 0 ]
then echo "Table mngtbl created successfully">>${LOG_FILENAME}
else echo "Table mngtbl already existed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Loading data from HDFS into hive table
hive -e "truncate table Project1.mngtbl;"
hive -e "load data inpath '/user/saif/project1/Input/tbl_project1_day/' into table Project1.mngtbl;"

#Checking whether data loaded to hive managed table
if [ $? -eq 0 ]
then echo "Data loaded to hive managed table successfully">>${LOG_FILENAME}
else echo "Data loading to hive managed table failed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Creating SCD-external table in hive
hive -e "
create external table Project1.scdtbl(
custid int,
username string,
quote_count string,
ip string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int,
Date_col date,
day int)
partitioned by(Year string,Month string)
row format delimited fields terminated by ',';"

#Checking whether the External (SCD) table created  in hive
if [ $? -eq 0 ]
then echo "Table scdtbl created successfully">>${LOG_FILENAME}
else echo "Table scdtbl already existed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Inserting data into scd table
hive -e "
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table Project1.scdtbl partition(Year,Month)
select custid,username,quote_count,ip,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,Date_col,day,Year,Month  from 
(select custid,username,quote_count,ip,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,Date_col,day,Year,Month,row_number() over(partition by custid order by Date_col desc)as rn from
(select * from Project1.scdtbl union select custid,username,quote_count,ip,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,Date_col,
cast(split(entry_time, '[/:]')[0] as int),split(entry_time,'[/:]')[2],split(entry_time, '[/:]')[1] from Project1.mngtbl)as a) b where rn=1;"

#Checking whether data loaded to hive managed table
if [ $? -eq 0 ]
then echo "Data inserted to hive SCD table successfully">>${LOG_FILENAME}
else echo "Data insertion to hive SCD table failed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Creating staging table 
hive -e "
create table  Project1.stgtbl(
custid int,
username string,
quote_count string,
ip string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int,
Date_col date,
day int,
Year string,
Month string)
row format delimited fields terminated by ',';"

#Checking whether the staging table created  in hive
if [ $? -eq 0 ]
then echo "Table stgtbl created successfully">>${LOG_FILENAME}
else echo "Table stgtbl already existed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#inserting only every day record to staging data
hive -e "
insert overwrite table Project1.stgtbl 
select * from Project1.scdtbl where Date_col=(select max(Date_col) from Project1.scdtbl);"

#Checking whether data loaded to hive managed table
if [ $? -eq 0 ]
then echo "Data inserted to hive Staging table successfully">>${LOG_FILENAME}
else echo "Data insertion to hive Staging table failed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Checking whether the Reconciliation table created  in Mysql
o=$(mysql -uroot -pWelcome@123 -e "select exists(select * from information_schema.tables where table_name='re_tbl');")
p=`echo "${o: -1}"`
if [ $p -eq 0 ]
then
#Creating table for re-conceilaton
mysql -uroot -pWelcome@123 -e "
create table if not exists Project1.re_tbl(
custid int,
username varchar(256),
quote_count varchar(256),
ip varchar(256),
prp_1 varchar(256),
prp_2 varchar(256),
prp_3 varchar(256),
ms varchar(256),
http_type varchar(256),
purchase_category varchar(256),
total_count varchar(256),
purchase_sub_category varchar(256),
http_info varchar(256),
status_code int,
Date_col date,
day int,
Year varchar(256),
Month varchar(256)
);"
echo "Table Reconciliation created successfully">>${LOG_FILENAME}
else echo "Table Reconciliation already existed">>${LOG_FILENAME}
hive -e "truncate table Project1.re_tbl;"
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Exporting data into the mysql table
sqoop export --connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=False --table re_tbl --username ${USERNAME} --password-file ${PASSWORD_FILE} --export-dir "/user/hive/warehouse/project1.db/stgtbl" --m 1 --driver com.mysql.jdbc.Driver --direct --input-fields-terminated-by ',' --input-null-string '\\N' --input-null-non-string '\\N'

#Checking whether data exported from  hive to mysql reconciliation table
if [ $? -eq 0 ]
then echo "Data exported to re_tbl table  in SQL successfully">>${LOG_FILENAME}
else echo "Data exported to re_tbl table failed">>${LOG_FILENAME}
fi
echo "---------------------------------------------------------------------------------------------------">>${LOG_FILENAME}

#Checking whether no.of records are same in mysql table and reconciliation table are equal
OP=`mysql -uroot -pWelcome@123 -e "select count(custid) from Project1.tbl_project1_day where custid not in (select custid from Project1.re_tbl);"`
if [ $? -eq 0 ]
then echo ${OP} >> ${LOG_FILENAME}
 echo "Equal no.of records">>${LOG_FILENAME}
else echo "No equal records">>${LOG_FILENAME}
fi




