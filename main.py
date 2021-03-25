#!/usr/bin/env python
# coding: utf-8

# In[192]:


import mysql.connector as conn,csv


# In[193]:


#create db connection
db=conn.connect(host="localhost",
                user="root",
                password=" ",
                database="Fisheries"
)
print("Succesfully connected to db:")


# In[194]:


#create cursor
cursor=db.cursor()


# In[195]:


#create database called fisheries
cursor.execute("CREATE DATABASE IF NOT EXISTS Fisheries")
print("The database created sucessfully!")


# In[196]:


#view databases
cursor.execute("SHOW DATABASES")
#show all databases
for database in cursor:
    print(database)


# In[197]:


#create boat table
cursor.execute('''CREATE TABLE IF NOT EXISTS Boat(
                boat_id INT PRIMARY KEY NOT NULL UNIQUE,
                boat_Name TEXT NOT NULL,
                boat_size DECIMAL(3,1) NOT NULL,
                boat_length DECIMAL(3,1) NOT NULL,
                station_id VARCHAR(30) NOT NULL,
                boat_capacity VARCHAR(30) NOT NULL,
                fishing TEXT NOT NULL); ''')
print("Boat table succesfully created!")


# In[198]:


#create fisher table
cursor.execute('''CREATE TABLE IF NOT EXISTS Fisher(
                fisherman_id INT PRIMARY KEY NOT NULL UNIQUE,
                fisher_names TEXT NOT NULL,
                boat_id  INT NOT NULL UNIQUE,
                phone_number VARCHAR(100) NOT NULL,
                email_address VARCHAR(100) NOT NULL,
                Age INT(100) NOT NULL,
                FOREIGN KEY (boat_id) REFERENCES Boat(boat_id) );''')
print("fisher table created!")


# In[199]:


#create Owner table
cursor.execute('''CREATE TABLE IF NOT EXISTS Owner(
                owner_id INT PRIMARY KEY NOT NULL UNIQUE,
                owner_name TEXT NOT NULL,
                boat_id INT  NOT NULL UNIQUE,
                phone_number VARCHAR(50) NOT NULL,
                email_address VARCHAR(50) NOT NULL,
                FOREIGN KEY (boat_id) REFERENCES Boat (boat_id) ); ''')
print("Owner table created")


# In[200]:


#create station table
cursor.execute('''CREATE TABLE IF NOT EXISTS Station(
                station_id INT PRIMARY KEY NOT NULL UNIQUE,
                station_name CHAR(20) NOT NULL,
                Address VARCHAR(30) NOT NULL); ''')
print("station table succesfully created! ")


# In[201]:


cursor.execute("SHOW TABLES")
for tables in cursor:
    print(tables)


# In[202]:


with open('C:\python\Assignment\mysql\Assignment2\Boat.csv', 'r') as boat_csv:
    reader = csv.reader(boat_csv)
    next(reader)
    for rows in reader:
        cursor.execute(
            "INSERT IGNORE INTO Boat (boat_id,boat_name,boat_size, boat_length, boat_capacity, station_id, fishing) VALUES ( %s, %s, %s, %s, %s ,%s,%s);", rows)
        db.commit()
print("Data uploaded sucessfully! ")


# In[203]:


#upload fisher csv files
with open('C:\python\Assignment\mysql\Assignment2\Fisher.csv', 'r') as fisher_csv:
    reader = csv.reader(fisher_csv)
    next(reader)
    for rows in reader:
        cursor.execute(
            "INSERT IGNORE INTO Fisher (fisherman_id, fisher_names, boat_id, phone_number, email_address, Age) VALUES ( %s, %s, %s, %s ,%s,%s);", rows)
        db.commit()
print("Data uploaded sucessfully! ")


# In[204]:


#create owner table
with open('C:\python\Assignment\mysql\Assignment2\Owner.csv', 'r') as owner_csv:
    reader = csv.reader(owner_csv)
    next(reader)
    for rows in reader:
        cursor.execute(
            "INSERT IGNORE INTO Owner (owner_id,owner_name, phone_number, email_address, boat_id) VALUES (%s, %s, %s ,%s, %s);", rows)
        db.commit()
print("Data uploaded sucessfully! ")


# In[205]:


#create station table
with open('C:\python\Assignment\mysql\Assignment2\Station.csv', 'r') as station_csv:
    reader = csv.reader(station_csv)
    next(reader)
    for rows in reader:
        cursor.execute(
            "INSERT IGNORE INTO Station (station_id, station_name, Address) VALUES (%s, %s ,%s);", rows)
        db.commit()
print("Data uploaded sucessfully! ")


# In[206]:



def fishery ():
    join = cursor.execute (''' SELECT *
                    FROM Boat
                    INNER JOIN Fisher
                    ON Boat.boat_id = Fisher.boat_id
                    INNER JOIN Owner
                    ON Owner.boat_id = Fisher.boat_id
                    INNER JOIN Station
                    ON Station.station_id = Boat.station_id;''')
    print("\t\t inner join result:\n \t\t ------------------\n")
    for rows in cursor.fetchall():
        print(rows,'\n')
fishery ()
cursor.close ()  


# In[ ]:




