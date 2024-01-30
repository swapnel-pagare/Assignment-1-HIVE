# Assignment-1-HIVE
1.  Download vechile sales data -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv
  ans: I downloaded file sales_order_data.csv and uploaded it on local file system of cloudera through filezilla.



2.Store raw data into hdfs location
  ans:  I used the following command to store raw data in an hdfs location "hadoop fs -copyFromLocal /home/cloudera/sales_order_data /sales_order_data"



3.Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table
  ans:
       create table sales_order_csv
       (
       ORDERNUMBER int,
       QUANTITYORDERED int,
       PRICEEACH float,
       ORDERLINENUMBER int,
       SALES float,
       STATUS string,
       QTR_ID int,
       MONTH_ID int,
       YEAR_ID int,
       PRODUCTLINE string,
       MSRP int,
       PRODUCTCODE string,
       PHONE string,
       CITY string,
       STATE string,
       POSTALCODE string,
       COUNTRY string,
       TERRITORY string,
       CONTACTLASTNAME string,
       CONTACTFIRSTNAME string,
       DEALSIZE string
       )
      row format delimited
      fields terminated by ',' 
      tblproperties("skip.header.line.count"="1")
      ;


    4.Load data from hdfs path into "sales_order_csv" 
     ans :I copied raw data from hdfs location to table sales_order_csv through command "load data inpath '/sales_order_csv/' into table sales_order_csv; in hive.

    5.Create an internal hive table which will store data in ORC format "sales_order_orc"
     ans :create table sales_order_orc
          (
          ORDERNUMBER int,
          QUANTITYORDERED int,
          PRICEEACH float,
          ORDERLINENUMBER int,
          SALES float,
          STATUS string,
          QTR_ID int,
          MONTH_ID int,
          YEAR_ID int,
          PRODUCTLINE string,
          MSRP int,
          PRODUCTCODE string,
          PHONE string,
          CITY string,
          STATE string,
          POSTALCODE string,
          COUNTRY string,
          TERRITORY string,
          CONTACTLASTNAME string,
          CONTACTFIRSTNAME string,
          DEALSIZE string
          )
          stored as orc;
     6. Load data from "sales_order_csv" into "sales_order_orc"
        ans: I loaded data from sales_data_csv to sales_order_orc with the command "insert overwrite table sales_order_orc select * from sales_order_csv;"

 Perform below menioned queries on "sales_order_orc" table :


      a. Calculate total sales per year
    ans: select year_id,sum(sales) as total_sales from sales_order_data_orc group by year_id;
         output is :year_id total_sales
                    2003    3516979.547241211
                    2004    4724162.593383789
                    2005    1791486.7086791992

                       
      b.   Find a product for which maximum orders were placed
    ans:   select productline as product,quantityordered as max_quantity_of_order from sales_order_orc group by productline order by quantityordered desc limit 1;
           output is :  product max_quantity_of_order
                        Classic Cars    97

      c.   Calculate the total sales for each quarter
    ans:   select qtr_id as quarter,sum(sales) as total_sale_per_quarter from sales_order_orc group by qtr_id;
           output is :quarter total_sale_per_quarter
                          1       2350817.726501465
                          2       2048120.3029174805
                          3       1758910.808959961
                          4       3874780.010925293

      d.   In which quarter sales was minimum 
    ans:   select qtr_id as quarter,sales as min_sale from sales_order_orc group by qtr_id,sales order by sales asc limit 1;
           output is :quarter min_sale
                      2       482.13
     
      e.   In which country sales was maximum and in which country sales was minimum
    ans:   1. the country in which sales was maximum 
         ans: select country,sales as country_in_which_sale_was_maximum from sales_order_orc order by country_in_which_sale_was_maximum desc limit 1;
              output is :country country_in_which_sale_was_maximum
                         USA     14082.8
           2. the country in which sales was minimum
         ans:   select country,sales as country_in_which_sale_was_minimum from sales_order_orc order by country_in_which_sale_was_minimum asc limit 1;
                output is :country country_in_which_sale_was_minimum
                           France  482.13
      
      f.   Calculate quartelry sales for each city                    
    ans:   select city,qtr_id as quarter,sum(sales) as sales from sales_order_orc group by city,qtr_id;
           output is very big.
      
      h. Find a month for each year in which maximum number of quantities were sold    
    ans:   select year_id,month_id ,sum(quantityordered) as total_quantity from sales_order_orc group by year_id, month_id order by year_id,total_quantity desc;
                   
