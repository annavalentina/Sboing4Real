**Sboing4Real** is a project that targets the development of crowdsourcing technologies for geo-social networking and advanced satellite navigation in real-time.
The apps in this repository detect traffic congestion and calculate statistics per road segment by collecting data from **Apache Druid** and **PostgreSQL** and analyze them using **Apache Spark**.


# Main Apps

***FreeFlowSpeed***

This app finds the free flow speed of every road segment. It starts by collecting data from Druid for a specific time period. Then it groups them by segment id and 5-min intervals and finds the average speed for each 5min interval ordered by speed for all the segments. Then it limits the results to only 5% (of each segment) and calculates their median. This results to the median of the top 5% of the fastest 5min intervals for each segment. Finally, it saves the results to PostgreSQL along with a timestamp for each entry in order to later delete the older entries.
After this app finishes, the " psqlDeleteDuplicatesFF.sh" needs to be called in order to remove the older entries.

***AverageSpeed***

This app finds the average speed for every hour of the day and for every road segment. Along with the average value it also finds a weighted average. In order to make the app more efficient instead of finding the average speed using all the data in Druid, only sum, count and average values are stored in the database and they are updated with the data of the past day. Using this technique, the app requests from Druid data for only one day at a time. For the weighted average data for only one day are requested from Druid as previously, but the new average speed is calculated using weights (old data participate 80% in the average and new data 20%). 
The app starts collecting data from Druid for a specific time period. Then it keeps only data that refer to the right day and groups them by segment id and hour. After that it recalls from PostgreSQL the previous count, sum and weighted average for all the segments for that specific day, merges them with the new ones and saves the results. If no data exist in PostgreSQL it just saves the results from Druid, in which case the weighted average is equal to the normal average.
The day is saved as an integer value between 1 and 7 (1 being Sunday) and the hour as an integer value between 0 and 23.
After this app finishes, the " psqlDeleteDuplicatesAvg.sh" needs to be called in order to remove the older entries.

## Arguments
The following arguments are necessary in order to run the apps:
1)Period in days (e.g. for 3 months of data the argument is 90 days)-For the average speeds usually this needs to be set to 1
2)Druid datasource name
3)Zookeeper hosts (for druid) 
4)PostgreSQL username
5)PostgreSQL password
6)PostgreSQL url 
7)PostgreSQL table name
Also, the apps need two external jars: druid-spark connector and PostgreSQL driver. These jars need to be placed in the master's file space and their paths can be added using the following argument: `--jars /path/to/jar,path/to/jar`. 

# PostgreSQL scripts
There exist four scripts that need to be called. Two of them are for the first app and two of them for the second one. All of them take the same input:
1) PostgreSQL username
2) PostgreSQL password
3) PostgreSQL host
4) PostgreSQL port
5) Database name
6) Table name

The first two scripts are called *psqlCreateTableFF* and *psqlCreateTableAvg*. They need to be called only once, in the start of the project in order to create the freeflowSpeed and averageSpeed tables respectively.
The other two are called *psqlDeleteDuplicatesFF* and *psqlDeleteDuplicatesAvg*. They both need to be called after the ending of the spark apps in order to remove older entries from the segments in the table.



# Running examples
***FreeFlowSpeed***
```
./spark-submit --master local[*] --jars /path/spark-druid-connector.jar,/path/postgresql-42.2.11.jar /pathToApp/ sparkdruid_freeflow_speed.jar 90 sboing_historical test1:2181,test2:2181 user pass jdbc:postgresql://test:5432/sboing freeflowpeeds
```
***AverageSpeed***
```
./spark-submit --master local[*] --jars /path/spark-druid-connector.jar,/path/postgresql-42.2.11.jar /pathToApp/ sparkdruid_average_speed.jar 1 sboing_historical test1:2181,test2:2181 user pass jdbc:postgresql://test:5432/sboing avgspeeds 
```
***PostgreSQL Scripts***
```
./ psqlCreateTableFF.sh user pass test 5432 sboing freeflowspeeds
./ psqlDeleteDuplicatesAvg.sh user pass test 5432 sboing avgspeeds
```

