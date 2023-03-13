

sqoop import-all-tables --connect jdbc:mysql://localhost:3306/healthcare --username root --password cloudera --hive-import --m 1


--problem statement4;
/*
The Healthcare department wants a report about the inventory of pharmacies.
Generate a report on their behalf that shows how many units of medicine each pharmacy has in their inventory, 
the total maximum retail price of those medicines, and the total price of all the medicines after discount. 
Note: discount field in keep signifies the percentage of discount on the maximum price.

*/

create external table ph_inv(pharmacyid int,variety_of_medicines int,total_units int,total_value float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


insert into table ph_inv
select D.pharmacyid ,count(D.medicineid) as variety_of_medicines,
sum(D.quantity) as total_units,
sum(D.totalval) as total_value
from
    (select 
    ph.pharmacyid,
    keep.medicineid,
    quantity,
    maxprice,
    discount,
    (quantity*maxprice)*(1-0.01*discount) as totalval from pharmacy ph left outer join keep on ph.pharmacyid=keep.pharmacyid join medicine on medicine.medicineid=keep.medicineid)  D
group by D.pharmacyid
order by total_value desc;



create table pharmacy_inventory(pharmacyid int,variety_of_medicines int,total_units int,total_value float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table pharmacy_inventory --export-dir /user/hive/warehouse/ph_inv --input-fields-terminated-by ','

---------------------------------------------------------------------------------------------------

---problem statement5;
/*
The healthcare department suspects that some pharmacies prescribe more medicines than others 
in a single prescription, for them, generate a report that finds for each pharmacy the maximum, 
minimum and average number of medicines prescribed in their prescriptions. 
*/

create external table med_prescri(pharmacyid int,avg_of_max_quantity float,avg_of_min_quantity float,avg_of_avg_quantity float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

insert into table med_prescri
select D.pharmacyid,
avg(D.max_quantity) as avg_of_max_quantity,
avg(D.min_quantity) as avg_of_min_quantity,
avg(D.avg_quantity) as avg_of_avg_quantity
from
    (select p.prescriptionid,p.pharmacyid,
    max(c.quantity) as max_quantity,
    min(c.quantity) as min_quantity,
    avg(c.quantity) as avg_quantity 
    from prescription p inner join contain c  on p.prescriptionid=c.prescriptionid 
    group by p.pharmacyid,p.prescriptionid 
    order by p.pharmacyid) D
 group by D.pharmacyid
 order by avg_of_avg_quantity;


create table med_prescri(pharmacyid int,avg_of_max_quantity float,avg_of_min_quantity float,avg_of_avg_quantity float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table med_prescri --export-dir /user/hive/warehouse/med_prescri --input-fields-terminated-by ','


--------------------------------------------------------------------------------

