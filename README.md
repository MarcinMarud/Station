Data Pipeline Project for Gas Station
Project Description

This project generates synthetic data for a gas station and processes it through a full ETL pipeline. I use Python to manage and control the database, including creating views and other database structures.

Then, I analyze the data using PostgreSQL queries to extract useful business insights. Finally, I build a dashboard in Power BI to visualize key metrics and trends.

Data Generation & Pipeline
Data generation was done by the faker library in Python. File main.py controls evrything from generation to refreshing dashboard. First data is generatef after that it is injected into database where the cleaning and transformating occurs after that it is analyzed
by postgresSQL queries and loaded into power bi file to refresh the data in the dashboard

ERD Schema

<img width="1548" height="708" alt="obraz" src="https://github.com/user-attachments/assets/626e3097-8e8d-41dd-be7e-3d19b55584fb" />

Database Views

<img width="470" height="139" alt="obraz" src="https://github.com/user-attachments/assets/f0e8956f-409e-4bae-91cd-44707466a27c" />

SQL Queries

<img width="826" height="574" alt="obraz" src="https://github.com/user-attachments/assets/8f4f373c-7a86-4924-8357-5decaa8f1810" />

Power BI Dashboard

<img width="1396" height="782" alt="obraz" src="https://github.com/user-attachments/assets/5b2b7ff5-1cb4-4312-af6d-e4675ba31493" />

Technologies Used

    Python

    PostgreSQL

    Power BI

How to Run

Get everything from repo then make sure everything from requirements is installed and run main.py 
