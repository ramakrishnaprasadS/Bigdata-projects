

--importing all required 13 tables from source database

sqoop import-all-tables --connect jdbc:mysql://localhost:3306/healthcare --username root --password cloudera --hive-import --m 1

---------------------------------------------------query1---------------------------------------------------------
--problem statement;
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
        maxprice,4v 
        discount,
        (quantity*maxprice)*(1-0.01*discount) as totalval from pharmacy ph left outer join keep on ph.pharmacyid=keep.pharmacyid join medicine on medicine.medicineid=keep.medicineid)  D
    group by D.pharmacyid
    order by total_value desc;


---in mysqldb
    create table pharmacy_inventory(pharmacyid int,variety_of_medicines int,total_units int,total_value float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table pharmacy_inventory --export-dir /user/hive/warehouse/ph_inv --input-fields-terminated-by ','

-----------------------------------------------------------query-2----------------------------------------

---problem statement;
/*
The healthcare department suspects that some pharmacies prescribe more medicines than others 
in a single prescription, for them, generate a report that finds for each pharmacy the maximum, 
minimum and average number of medicines prescribed in their prescriptions. 
*/

    create external table med_prescri(pharmacyid int,avg_of_max_quantity float,avg_of_min_quantity float,avg_of_avg_quantity float)
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY ','
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

--in mysqldb
    create table med_prescri(pharmacyid int,avg_of_max_quantity float,avg_of_min_quantity float,avg_of_avg_quantity float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table med_prescri --export-dir /user/hive/warehouse/med_prescri --input-fields-terminated-by ','


---------------------------------------------------------------query-3----------------------------------

/*Problem Statement: 
A company needs to set up 3 new pharmacies, 
they have come up with an idea that the pharmacy can be set up in 
cities where the pharmacy-to-prescription ratio is the lowest and the number of prescriptions 
should exceed 100. 
Assist the company to identify those cities where the pharmacy can be set up.*/


    create external table city_pharmacy(city string,prescription_cnt int,pharmacy_cnt int,prescr_pharmacy_ratio float)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

    insert into table city_pharmacy
    select a.city,
    count(pr.prescriptionid) as pres_cnt,
    count(distinct p.pharmacyid) as pharmacy_cnt, 
    count(pr.prescriptionid)/count(distinct p.pharmacyid) as prescr_pharmacy_ratio 
    from address a left outer join pharmacy p on a.addressid=p.addressid
    inner join prescription pr on p.pharmacyid=pr.pharmacyid 
    group by a.city 
    having pres_cnt>100
    order by prescr_pharmacy_ratio desc;

--in mysqldb
    create table city_pharmacy(city varchar(20),prescription_cnt int,pharmacy_cnt int,prescr_pharmacy_ratio float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table city_pharmacy --export-dir /user/hive/warehouse/city_pharmacy --input-fields-terminated-by ','




--------------------------------------------------------query-4----------------------------
/*
Problem Statement: 
“HealthDirect” pharmacy finds it difficult to deal with the product type of medicine 
being displayed in numerical form, they want the product type in words. 
Also, they want to filter the medicines based on tax criteria. 
Display only the medicines of product categories 1, 2, and 3 for medicines 
that come under tax category I and medicines of product categories 4, 5, and 6 for
medicines that come under tax category II.
Write a SQL query to solve this problem.
ProductType numerical form and ProductType in words are given by
1 - Generic, 
2 - Patent, 
3 - Reference, 
4 - Similar, 
5 - New, 
6 - Specific,
7 - Biological, 
8 – Dinamized

3 random rows and the column names of the Medicine table are given for reference.
Medicine (medicineID, companyName, productName, description, substanceName, productType, taxCriteria, hospitalExclusive, governmentDiscount, taxImunity, maxPrice)

*/

    create external table HD_pharmacy(medicineID int,companyName string,productName string,description string,substanceName string,Product_Type string,taxCriteria string,hospitalExclusive string,governmentDiscount string,taxImunity string,maxPrice float)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';
    
    insert into table HD_pharmacy 
    select m.medicineID,m.companyName,m.productName,m.description,m.substanceName,
    case m.productType
        when 1 then "Genereic"
        when 2 then "Patent"
        when 3 then "Reference"
        when 4 then "Similar"
        when 5 then "New"
        when 6 then "Specific"
        when 7 then "Biological"
        when 8 then "Dinamized"
    end as Product_Type,
    m.taxCriteria,m.hospitalExclusive,m.governmentDiscount,m.taxImunity,m.maxPrice
    from 
    pharmacy ph inner join keep k on ph.pharmacyid=k.pharmacyid
    inner join medicine m on k.medicineid=m.medicineid
    where ph.pharmacyName="HealthDirect" and 
    ((m.productType in (1,2,3) and m.taxCriteria="I") or (m.productType in (4,5,6) and m.taxCriteria="II") )
    ORDER BY m.taxCriteria;

--in mysqldb
    create table HD_pharmacy(medicineID int,companyName varchar(100),productName varchar(100),description varchar(100),substanceName varchar(100),Product_Type varchar(30),taxCriteria varchar(10),hospitalExclusive varchar(10),governmentDiscount varchar(10),taxImunity varchar(10),maxPrice float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table HD_pharmacy --export-dir /user/hive/warehouse/hd_pharmacy --input-fields-terminated-by ','


-----------------------------------------------query5--------------------------------------

Problem Statement:  
Sarah, from the healthcare department, has noticed many people do not claim insurance for 
their treatment. She has requested a state-wise report of the percentage of treatments that
 took place without claiming insurance. Assist Sarah by creating a report 
 as per her requirement.
*/


    create external table statewise_unclaimed(state string,total_treatments_notClaimed int,total_treatments int,unClaimed_percentage float)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

    insert into table statewise_unclaimed
    select a.state,
    count(t.treatmentid)-count(t.claimid) as total_treatments_notClaimed,
    count(t.treatmentid) as total_treatments,
    ((count(t.treatmentid)-count(t.claimid))/count(t.treatmentid))*100 as  unClaimed_percentage
    from 
    address a left outer join person p on a.addressid=p.addressid
    inner join treatment t on p.personid = t.patientid
    group by a.state;

--in mysqldb
    create table statewise_unclaimed(state varchar(20),total_treatments_notClaimed int,total_treatments int,unClaimed_percentage float);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table statewise_unclaimed --export-dir /user/hive/warehouse/statewise_unclaimed --input-fields-terminated-by ','


------------------------------------------------------query 6---------------------------------

/*
Problem Statement: 
In the Inventory of a pharmacy 'Spot Rx' the quantity of medicine is considered ‘HIGH QUANTITY’ 
when the quantity exceeds 7500 
and ‘LOW QUANTITY’ when the quantity falls short of 1000. The discount is considered “HIGH” 
if the discount rate on a product is 30% or higher, and the discount is considered “NONE” 
when the discount rate on a product is 0%.
 'Spot Rx' needs to find all the Low quantity products with high discounts and all the high-quantity 
 products with no discount so they can adjust the discount rate according to the demand. 
Write a query for the pharmacy listing all the necessary details relevant to the given requirement.

Hint: Inventory is reflected in the Keep table.

*/

    create external table medicine_status(medicineid int,quantity int,qty_status string,discount_status string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

    insert into table medicine_status
    select k.medicineid ,k.quantity, 
        case 
            when k.quantity>7500 then "HIGH QUANTITY"
            when k.quantity<1000 then "LOW QUANTITY"
            else "OK"
            end as qty_status,
        case 
            when k.discount>30 then "HIGH"
            when k.discount=0 then "NONE"
            else "NORMAL"
            end as discount_status
        from keep k inner join pharmacy ph on k.pharmacyid=ph.pharmacyid 
        where ph.`pharmacyName`="Spot Rx"
        and ( (k.quantity<1000 and k.discount>30) or (k.quantity>7500 and k.discount=0) );

--in mysqldb
    create table medicine_status(medicineid int,quantity int,qty_status varchar(50),discount_status varchar(50));

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table medicine_status --export-dir /user/hive/warehouse/medicine_status --input-fields-terminated-by ','



----------------------------------------------------------query 7-------------------------

problem statement: 
-- The total quantity of medicine in a prescription is the sum of the quantity of all the medicines in the prescription.
-- Select the prescriptions for which the total quantity of medicine exceeds
-- the avg of the total quantity of medicines for all the prescriptions.

    create external table prescri_medcount(prescriptionid int,tot_qty int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

    insert into table prescri_medcount
    select prescriptionid,tot_qty from
        (select pr.prescriptionid,sum(c.quantity) as tot_qty,
        avg(sum(c.quantity)) over() as avg_qty
        from 
        pharmacy ph inner join Prescription pr on ph.pharmacyid=pr.pharmacyid
        inner join contain c on c.prescriptionid=pr.prescriptionid
        group by pr.prescriptionid) D
    where tot_qty > avg_qty;

--in mysqldb:
    create table prescri_medcount(prescriptionid int,tot_qty int);

sqoop export --connect jdbc:mysql://localhost:3306/results --username root --password cloudera --table prescri_medcount --export-dir /user/hive/warehouse/prescri_medcount --input-fields-terminated-by ','

-------------------------------------------------------------




































