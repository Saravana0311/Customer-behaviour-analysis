# -*- coding: utf-8 -*-
"""
Created on Tue Mar 18 11:42:28 2025

@author: User
"""
import pandas as pd 
import mysql.connector as db

host_name = "localhost" 
user_name = "dhamodaran"
password  = ""
db_name   = "mm"

#Connection String for MySQL

db_connection = db.connect (
    host     = host_name,
    user     = user_name,
    password = password,
    database = db_name       
    )

print("Local host Connection")

curr = db_connection.cursor()

#Customer journey csv input file 
customer_journey = pd.read_csv("D:\\Python\\Project\\Customer Behaviour Analysis\\Input File\\customer_journey.csv")

#Cleansing the data  handling the null values 

customer_journey.fillna(0).isnull().sum()

customer_journey_etl = customer_journey.fillna(0)

SQL = """drop table if exists mm.guvi_customer_journey"""

curr.execute(SQL)

SQL_CREATE_TABLE ="""Create  table guvi_customer_journey
( JourneyID       int,
 CustomerID      int,
 ProductID       int,
 VisitDate      date,
 Stage          varchar(100),
 Action         varchar(100),
 Duration      float);"""

curr.execute(SQL_CREATE_TABLE)

insert = """
insert into mm.guvi_customer_journey 
values 
(%s,%s,%s,%s,%s,%s,%s)
"""
# Convert DataFrame to List of Tuples

data_to_insert = [tuple(row) for row in customer_journey_etl.itertuples(index=False)]

# Execute Batch Insert

curr.executemany(insert, data_to_insert)

# Commit and Close Connection

db_connection.commit()

print("customer_journey Data successfully inserted into MySQL!")


#******************Customer journey csv input file ************************************************

customer_reviews = pd.read_csv("D:\\Python\\Project\\Customer Behaviour Analysis\\Input File\\customer_reviews.csv")

customer_reviews.isnull().sum()

SQL = """drop table mm.guvi_customer_reviews"""

curr.execute(SQL)
 
SQL_CREATE_TABLE ="""Create  table guvi_customer_reviews
(ReviewID       int,
CustomerID     int,
ProductID      int,
ReviewDate    date,
Rating         int,
ReviewText    Varchar(200));"""

curr.execute(SQL_CREATE_TABLE)

insert = """
insert into mm.guvi_customer_reviews 
values 
(%s,%s,%s,%s,%s,%s)
"""
# Convert DataFrame to List of Tuples

data_to_insert = [tuple(row) for row in customer_reviews.itertuples(index=False)]

# Execute Batch Insert

curr.executemany(insert, data_to_insert)

# Commit and Close Connection

db_connection.commit()

print("customer_reviews Data successfully inserted into MySQL!")

#*******************Customer journey csv input file ************************************************

customers = pd.read_csv("D:\\Python\\Project\\Customer Behaviour Analysis\\Input File\\customers.csv",delimiter=',')

customers.isnull().sum()

SQL = """drop table mm.guvi_customers"""

curr.execute(SQL)
 
SQL_CREATE_TABLE =""" create table mm.guvi_customers
(CustomerID       int ,
CustomerName    varchar(100),
Email           varchar(100),
Gender          varchar(10),
Age              int,
GeographyID      int);"""

curr.execute(SQL_CREATE_TABLE)

insert = """ 
insert into mm.guvi_customers
values
(%s,%s,%s,%s,%s,%s)
"""
# Convert DataFrame to List of Tuples

data_to_insert = [tuple(row) for row in customers.itertuples(index=False ,name=None)]

# Execute Batch Insert3

curr.executemany(insert,data_to_insert)

# Commit and Close Connection

db_connection.commit()


print("customers Data successfully inserted into MySQL!")

#*******************************engagement_data csv input file ************************************

engagement_data = pd.read_csv("D:\\Python\\Project\\Customer Behaviour Analysis\\Input File\\engagement_data.csv")

engagement_data.isnull().sum()

SQL_CREATE_TABLE=""" drop TABLE MM.guvi_engagement_data"""

curr.execute(SQL_CREATE_TABLE)

SQL_CREATE_TABLE=""" CREATE TABLE MM.guvi_engagement_data
(EngagementID            int,
ContentID               int,
ContentType             varchar(100),
Likes                   int,
EngagementDate          varchar(100),
CampaignID              int,
ProductID               int,
ViewsClicksCombined    varchar(100))
"""

curr.execute(SQL_CREATE_TABLE)

INSERT_QUERY = """ 
INSERT INTO mm.guvi_engagement_data (EngagementID, ContentID, ContentType, Likes, EngagementDate, CampaignID, ProductID, ViewsClicksCombined)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

data_to_insert = [tuple(row) for row in engagement_data.itertuples(index=False,name=None)]

curr.executemany(INSERT_QUERY,data_to_insert)

# Commit and Close Connection
db_connection.commit()


print("engagement -- Data successfully inserted into MySQL!")

#*******************************geography csv input files ***************************************

geography = pd.read_csv("D:\\Python\\Project\\Customer Behaviour Analysis\\Input File\\geography.csv")

geography.isnull().sum()


SQL_CREATE_TABLE=""" drop TABLE IF EXISTS MM.guvi_geography"""

curr.execute(SQL_CREATE_TABLE)

SQL_CREATE_TABLE=""" CREATE TABLE MM.guvi_geography
(GeographyID     int,
Country        varchar(100),
City           varchar(100))
"""

curr.execute(SQL_CREATE_TABLE)

INSERT_QUERY = """ 
INSERT INTO mm.guvi_geography 
VALUES (%s, %s, %s)
"""

data_to_insert = [tuple(row) for row in geography.itertuples(index=False,name=None)]

curr.executemany(INSERT_QUERY,data_to_insert)

# Commit and Close Connection
db_connection.commit()


print("geography -- Data successfully inserted into MySQL!")


#********************************products csv input files *******************************************

products = pd.read_csv("D:\\Python\\Project\\Customer Behaviour Analysis\\Input File\\products.csv")

products.isnull().sum()


SQL_CREATE_TABLE=""" drop TABLE if exists MM.guvi_products"""

curr.execute(SQL_CREATE_TABLE)

SQL_CREATE_TABLE=""" CREATE TABLE MM.guvi_products
(ProductID        int,
ProductName     varchar(100),
Category        varchar(100),
Price          float)
"""

curr.execute(SQL_CREATE_TABLE)

INSERT_QUERY = """ 
INSERT INTO mm.guvi_products 
VALUES (%s, %s, %s,%s)
"""

data_to_insert = [tuple(row) for row in products.itertuples(index=False,name=None)]

curr.executemany(INSERT_QUERY,data_to_insert)

# Commit and Close Connection
db_connection.commit()

print("Products -- Data inserted successfully!")

print("File Upload Completed")




#******************************File Upload Completed ***********************
"""*/ Table List 
mm.guvi_customer_journey;
mm.guvi_customer_reviews
mm.guvi_customers
mm.guvi_engagement_data
mm.guvi_geography
mm.guvi_products
/*
"""
#Customer Journey & Engagement Analysis (SQL): 

#1) Identify drop-off points in the customer journey.

SQL ="""SELECT Stage, COUNT(CustomerID) AS DropOffCount
FROM mm.guvi_customer_journey
WHERE Action != 'Purchase'
GROUP BY Stage
ORDER BY DropOffCount DESC;"""

curr.execute(SQL)    
dropoff = curr.fetchall()

dropoff_table =pd.DataFrame(dropoff,columns=curr.column_names)

print('drop-off points')

#2) Find common actions leading to successful conversions.

SQL ="""SELECT a.Action,b.productname ,COUNT(*) AS Frequency
FROM mm.guvi_customer_journey a
JOIN mm.guvi_products b ON a.ProductID = b.ProductID
WHERE a.Stage = 'Checkout' AND a.Action <> 'Drop-off'
GROUP BY a.Action,b.productname
ORDER BY Frequency DESC;"""

curr.execute(SQL)    
commonactions = curr.fetchall()

commonactions_table =pd.DataFrame(commonactions,columns=curr.column_names)
print('Successful conversions')

#3) Calculate average duration per stage for engagement insights.

SQL ="""SELECT Stage, AVG(Duration) AS Avg_Duration
	FROM mm.guvi_customer_journey
	GROUP BY Stage
	ORDER BY Avg_Duration DESC;;"""

curr.execute(SQL)    
Avgduration = curr.fetchall()

Avgduration_table =pd.DataFrame(Avgduration,columns=curr.column_names)

print('Calculate average duration per stage')

#*********************Customer Reviews Analysis (SQL & Python):***************************

SQL="""
WITH RATING_CTE AS (
    SELECT  
        B.PRODUCTNAME, 
        B.CATEGORY, 
        AVG(A.RATING) AS AvgRating
    FROM MM.GUVI_CUSTOMER_REVIEWS A  
    LEFT JOIN MM.GUVI_PRODUCTS B ON A.PRODUCTID = B.PRODUCTID
    GROUP BY B.PRODUCTNAME, B.CATEGORY
),
LOWRATING AS (
    SELECT PRODUCTNAME, CATEGORY, AVGRATING, 'LOWRATING' AS RATINGSCALE
    FROM RATING_CTE
    ORDER BY AvgRating ASC
    LIMIT 1
),
HIGHRATING AS (
    SELECT PRODUCTNAME, CATEGORY, AVGRATING, 'HIGHRATING' AS RATINGSCALE
    FROM RATING_CTE
    ORDER BY AvgRating DESC
    LIMIT 1
)
SELECT * FROM LOWRATING
UNION ALL 
SELECT * FROM HIGHRATING;"""

curr.execute(SQL)

ratings = curr.fetchall()

ratings_final =pd.DataFrame(ratings,columns=curr.column_names)

SQL="""
select * from mm.guvi_customer_reviews a left join mm.guvi_products b
on a.productID=b.productID
"""

curr.execute(SQL)

data = curr.fetchall()

SentimentAnalysis = pd.DataFrame(data,columns=curr.column_names)

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# Initialize VADER Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Function to compute sentiment
def get_sentiment(text):
    score = analyzer.polarity_scores(text)['compound']
    if score > 0:
        return 'Positive'
    elif score < 0:
        return 'Negative'
    else:
        return 'Neutral'
# Apply sentiment analysis
SentimentAnalysis['Sentiment'] = SentimentAnalysis['ReviewText'].apply(get_sentiment)


#***** to find the negative review products names ************

negativereview= (SentimentAnalysis[SentimentAnalysis['Sentiment']=='Negative']
                 .groupby('ProductName')
                 .agg({'ProductName':'count'}).rename(columns={'ProductName': 'TCount'}))

Review_top3 = (SentimentAnalysis[SentimentAnalysis['Sentiment'] == 'Negative']
               .groupby('ReviewText')
               .size()
               .reset_index(name='Count')   
               .sort_values(by='Count', ascending=False)   
               .head(3)   
               .reset_index(drop=True)) 

# Display results
SentimentAnalysis_alt =(SentimentAnalysis.groupby(['Sentiment','Rating']).
                        agg({'Sentiment': 'count'}).
                        rename(columns={'Sentiment': 'TCount'}))

#***** Pivot the Sentiment values and Rating ************

SentimentAnalysis_pivot = SentimentAnalysis_alt.pivot_table(
    values='TCount', 
    index='Sentiment', 
    columns='Rating', 
    aggfunc='sum',
    fill_value=0  
)

print('SentimentAnalysis,negativereview,Review_top3,SentimentAnalysis_pivot Completed!')

#************************Marketing Effectiveness (SQL):*******************************************


SQL ="""
WITH repeat_customers AS (
    SELECT CustomerID, COUNT(*) AS purchase_count
    FROM mm.guvi_customer_journey
    GROUP BY CustomerID
    HAVING COUNT(*) > 1
)
SELECT 
    (COUNT(DISTINCT r.CustomerID) * 100.0 / COUNT(DISTINCT c.CustomerID)) AS RetentionRate
FROM mm.guvi_customers c
LEFT JOIN repeat_customers r ON c.CustomerID = r.CustomerID;
"""

curr.execute(SQL)

data=curr.fetchall()

retention = pd.DataFrame(data,columns=curr.column_names)


SQL ="""
SELECT 
    CASE WHEN purchase_count > 1 THEN 'Repeat Buyer' ELSE 'First-Time Buyer' END AS BuyerType,
    COUNT(*) AS TotalCustomers
FROM (
    SELECT CustomerID, COUNT(*) AS purchase_count
    FROM mm.guvi_customer_journey
    WHERE Stage = 'Checkout'
    GROUP BY CustomerID
) AS customer_purchases
GROUP BY BuyerType;
"""

curr.execute(SQL)

data=curr.fetchall()

repeatedcustomers = pd.DataFrame(data,columns=curr.column_names)

# Best-Performing Products Per Region

SQL ="""
SELECT 
    g.country ,
    COUNT(j.ProductID) AS SalesCount
FROM mm.guvi_customer_journey j
JOIN mm.guvi_products p ON j.ProductID = p.ProductID
JOIN mm.guvi_customers c ON j.CustomerID = c.CustomerID
JOIN mm.guvi_geography g ON c.GeographyID = g.GeographyID
WHERE j.Stage = 'Checkout'
GROUP BY g.country
ORDER BY  SalesCount desc;
"""

curr.execute(SQL)

data=curr.fetchall()

Region = pd.DataFrame(data,columns=curr.column_names)

###### Blog View ,likes & Clicks

SQL="""SELECT 
    ContentType, b.ProductName,
    SUM(likes) AS TotalLikes, 
    sum(SUBSTRING_INDEX(ViewsClicksCombined, '-', 1)) AS Views ,
     sum(SUBSTRING_INDEX(ViewsClicksCombined, '-', -1)) AS Clicks
FROM mm.guvi_engagement_data A left join mm.guvi_products b on a.productId=b.ProductID
GROUP BY ContentType , b.ProductName
"""

curr.execute(SQL)

data=curr.fetchall()

viewsvsclick = pd.DataFrame(data,columns=curr.column_names)

#viewsvsclick['ContentType']=viewsvsclick['ContentType'].str.replace('blog','b')

viewsvsclick1=viewsvsclick.drop('ProductName',axis=1)

viewsvsclick_pivot = viewsvsclick.pivot_table(
    index='ProductName', 
    columns='ContentType',
    values=['TotalLikes','Views','Clicks'], 
    aggfunc='sum'   
)
