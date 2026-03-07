## Class: Aircraft
### Responsibilities
* stores aircraft information
### Collaborators:
* 
### Assumptions:
* 

## Class: Aircraft_Listener
### Responsibilities:
* listens for aircraft transponder packets
* unpacks and formats data
* calls DB manager to add data to database
### Collaborators:
* DB_Manager
### Assumptions:
* 

## Class: DB_Manager
### Responsibilities:
* accepts and stores data from Aircraft_Listener
* returns query data for Display_Fetch, DB_Query and Safety_Analyzer
### Collaborators:
* Aircraft_Listener
* DB_Query
* Safety_Analyzer
* DB_Query
### Assumptions:
* 

## Class: Display_Fetch
### Responsibilities:
* fetch data via DB_Manager every 10 secs for controller console
* builds graphical interface from query data and alerts
### Collaborators:
* DB_Manager
### Assumptions:
* 

## Class: Safety_Analyzer
### Responsibilities:
* fetch data from database periodically with DB_Manager
* compare and analyze the data for dangerous situations
* send alerts to Display_Fetch if risk is detected
### Collaborators:
* DB_Manager
* Display_Fetch
### Assumptions:
* 

## Class: DB_Query
### Responsibilities:**
* accepts queries from console
* fetches data from DB_Manager
* passes the data to Display_Fetch
### Collaborators:
* DB_Manager
* Display_Fetch
### Assumptions:
* 


