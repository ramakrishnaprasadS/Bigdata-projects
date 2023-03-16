

------------------------------------------------------------query 8--------------------------------------------------
/*
Problem Statement: 
The State of Alabama (AL) is trying to manage its healthcare resources more efficiently.
For each city in their state, they need to identify the disease for which 
the maximum number of patients have gone for treatment. Assist the state for this purpose.
Note: The state of Alabama is represented as AL in Address Table.

*/

    ----------address table partition---------


    CREATE TABLE IF NOT EXISTS address_part (addressid int,address1 string,city string,zip int)
        COMMENT 'Address_partition'
        PARTITIONED BY (state string) 
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n';

    insert into address_part partition(state) select addressid ,address1 ,city,zip,state from address;

    --------------------------------------------

        
    create external table AL_treatcount(city string,diseaseid int,treat_cnt int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

    insert into table AL_treatcount
    select city,diseaseid,treat_cnt
    from
        (select a.city,t.diseaseid,count( t.treatmentid) as treat_cnt,
        dense_rank() over(partition by a.city order by count( t.treatmentid) desc) as drnk
        from treatment t inner join person p on t.patientid=p.personid  
        inner join address_part a on p.addressid=a.addressid 
        where a.state="AL" 
        group by a.city,t.diseaseid
        order by a.city asc ) D  
    where drnk=1
    order by treat_cnt desc;

--in mysqldb:
    create table AL_treatcount(city varchar(50),diseaseid int,treat_cnt int);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table AL_treatcount --export-dir /user/hive/warehouse/al_treatcount --input-fields-terminated-by ','


-------------------------------------------query 9-------------------------

/*Problem Statement: 
The healthcare department wants a pharmacy report on the percentage of hospital-exclusive 
medicine prescribed in the year 2022.
Assist the healthcare department to view for each pharmacy, 
the pharmacy id, pharmacy name, total quantity of medicine prescribed in 2022,
total quantity of hospital-exclusive medicine prescribed by the pharmacy in 2022,
 and the percentage of hospital-exclusive medicine to the total medicine prescribed in 2022.
Order the result in descending order of the percentage found. 
*/

    -----------partition & buckets on treatment----------------

    create table if not exists treatment_part_buckt
    (
    treatmentid int,
    date string,
    patientid int,
    diseaseid int,
    claimid int
    )
    partitioned by (year string)
    clustered by (treatmentid) into 3 buckets
    row format delimited
    fields terminated by ','
    stored as textfile;

    insert into treatment_part_bkt
    partition(year)
    select treatmentid,date,patientid,diseaseid,claimid,year(date) as year from treatment;

    insert into treatment_part_buckt partition(year) select treatmentid,date,patientid,diseaseid,claimid,year(date) as year from treatment;

    --------------------------------------------------------

    create external table hex_medstatus(pharmacyname string,total_quantity_2022 int,HEX_quantity_2022 int,HEX_medicine_percent float)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';


    with cte as
        (select ph.pharmacyname,
        sum(c.quantity) as total_quantity_2022,
        sum(if(m.hospitalExclusive="S",c.quantity,0)) as HEX_quantity_2022
        from 
        pharmacy ph inner join prescription pr on ph.pharmacyid=pr.pharmacyid
        inner join treatment_part_buckt t on t.treatmentid=pr.treatmentid
        inner join contain c on c.prescriptionid=pr.prescriptionid
        inner join medicine m on m.medicineid=c.medicineid
        where year(t.date)=2022
        group by ph.pharmacyname
        order by ph.pharmacyname)
    insert into table hex_medstatus
    select pharmacyname,total_quantity_2022,HEX_quantity_2022,
    (HEX_quantity_2022*100)/total_quantity_2022 as HEX_medicine_percent
    from cte
    order by HEX_medicine_percent desc;

--in mysqldb

    create table hex_medstatus(pharmacyname varchar(50),total_quantity_2022 int,HEX_quantity_2022 int,HEX_medicine_percent float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table hex_medstatus --export-dir /user/hive/warehouse/hex_medstatus --input-fields-terminated-by ','



---------------------------------------query 10-------------------------------------------

/*Problem Statement:  
Jhonny, from the finance department of Arizona(AZ), has requested a report that lists the 
total quantity of medicine each pharmacy in his state has prescribed that falls 
under Tax criteria I for treatments that took place in 2021. Assist Jhonny 
in generating the report. */

    create external table az_treatments(pharmacyname string,total_qty int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

    insert into table az_treatments
    select ph.pharmacyname,sum(c.quantity) as total_quantity
    from 
    address a inner join pharmacy ph on a.addressid=ph.addressid
    inner join prescription pr on ph.pharmacyid=pr.pharmacyid
    inner join treatment_part_buckt t on pr.treatmentid=t.treatmentid
    left outer join contain c on c.prescriptionid=pr.prescriptionid
    inner join medicine m on m.medicineid=c.medicineid
    where a.state="AZ" and m.taxcriteria="I" and year(t.date)=2021
    group by ph.pharmacyname
    order by total_quantity desc;


--in mysqldb:
    create table az_treatments(pharmacyname varchar(50),total_qty int);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table az_treatments --export-dir /user/hive/warehouse/az_treatments --input-fields-terminated-by ','


---------------------------------------------------------------------------------------------------
