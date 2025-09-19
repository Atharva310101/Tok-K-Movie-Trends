# Project 3: Data Science with Spark

* Team Name: team-05
* Author 1: Atharva Pargaonkar
* Author 2: #######
* Class: CS535 Section 1
* Semester: Fall 2023

## Overview
This project is an implementation of Spark for solving data science problem related to movie ratings. It works on the MovieLens 1 Million data set and uses PySpark dataframes to analyze six interesting trends in the data set, which are listed below:

1. Top K Movies of all time (based on average ratings, for movies with at least 500 user ratings) 
2. Top K Movies for a specific Age Group (age group 18-24, based on count)
3. Top K Movies for a specific Season (Spring, based on count)
4. Top K Movies for users with a specific Occupation (executive/managerial, based on count) 
5. Top K Movies for each Gender (based on count)
6. Top K Movies for each Genre (based on count)

## Reflection
This project of analyzing 'Top K Trends' is a classic example of combining Data Science and
Large Scale Data Analysis. To solve the data science problem involved throughout this project,
we employed a systematic and versatile methodology. By leveraging Apache Spark for distributed
data processing, loading and merging datasets comprising user ratings, demographic information,
and movie details we were able to achieve the goal for this project in an efficient way. It
mainly consisted of transforming the data, renaming columns, and mapping categorical variables. We
utilized various functions in Spark to calculate average ratings, movie popularity, and dynamically
adjusted parameters such as age ranges and gender groups. While we're aware of the potential
issues with using coalesce in PySpark, such as loss of parallelism and increased memory overhead,
we've employed it intentionally to produce a single CSV file as output for each trend.

The workflow of the project starts with joining the three input data files ratings.dat,
movies.dat and users.dat in a single dataframe. This dataframe is then used in each trend for
performing trend-specific data processing, grouping and sorting. This helped us to display the
top - k values of each trend according to the requirements. We were able to generate the output
in the terminal as well as write it to separate csv files for each of the trend.

The overall execution of the project was quite smooth. The initial approach was to make a
separate python script for each of the trend. The spark documentation on using sql functions was
helpful as we could explore various libraries and functions, which made the execution simpler. On
successful execution of individual scripts, we created a single combined Top_K_trends.py file
so that we could run all the trends at once.

There were definitely some challenges that we encountered while working on the project. For
the trend that finds top k movies for a particular season, we needed to work with timestamps,
and converting it to season was difficult for us. We wrote a user-defined function to convert
months to seasons, registered it as a UDF, and included a function to add a "Season" column to
a DataFrame based on timestamps.

The other challenge we faced was verifying the correctness of our results. We always had correct
answers for the other projects we worked on previously. So for this project, figuring out a
way to assess the correctness was quite tedious. <br> 

*(How we assessed the correctness is described in the **Results** section).*

In two of the trends, the results were not matching with what we got through our assessment. So
we reanalyzed our functions, fixed the logic and ended with correct results.

Therefore, the project gave us an in-depth understanding of dataframes in spark and how to work
on large datasets efficiently.

**Work Contribution**

We had multiple virtual meeting sessions where we discussed and worked on the project together,
so most efforts and tasks were collaborative. Both of us contributed equally in terms of effort
and hours spent on the project. Some areas where we majorly contributed are:<br>

- Atharva Pargaonkar: Creating scripts for 4 trends and organizing the code after combining to a single python file
- Shrutee Dwa: Creating scripts for 2 trends, creating sample data set, verifying the correctness of results and making changes to the code logic as required

## Compiling and Using
Follow the steps below to build and use the code.

The command to run the program should have the following format: <br>

spark-submit --master local[*] Top_k_trends.py <value_of_k> <input_data_path> <output_data_path> 2> /dev/null

**Usage: For a Spark cluster that is up and running but using shared local filesystem , use (here, k=10):**
```
spark-submit --master local[*] Top_k_trends.py 10 input_dataset output_dataset 2>/dev/null
```

**Usage: For a Spark cluster that is up and running along with a Hadoop cluster, use:**
Put the dataset in the hdfs using the command:
```
hdfs dfs -put input_dataset
```
Submit the task to Spark
```
spark-submit --master spark://cscluster00:7077 Top_k_trends.py 10 hdfs://cscluster00:9000/user/shruteedwa/input_dataset hdfs://cscluster00:9000/user/shruteedwa/output_dataset 2>/dev/null
```

## Results 

The output of the trends are displayed in the console as well as written to individual csv files. 

**Assessment of the correctness of the results**
For the purpose assessment, we used the small data set. We created a csv file combining the three data files (movies.dat, users.data, ratings.dat) of the small data set, which we then used in Tableau to verify the results of the trends.

**1. Top K Movies of all time (based on average ratings, for movies with at least 500 user ratings)**

   Output from the program:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/5cbe48da-234b-4998-a6dd-67b48581bba4" width="600" alt="Output from the program:">

   Output on Tableau:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/29f53d8c-95f9-451e-962f-3741dfd70d37" width="600" alt="Description of your image"><br>

**2. Top K Movies for a specific Age Group (age group 18-24, based on count)**

   Output from the program:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/70e6a24a-6b8f-4273-ac47-80d1acda7a62" width="600" alt="Description of your image">

   Output on Tableau:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/ddceb178-0114-48ee-a65e-74aeef084652" width="600" alt="Description of your image"><br>

**3. Top K Movies for a specific Season (Spring, based on count)**

   Output from the program:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/bf58801f-c43f-455c-ae33-99c307ef301b" width="600" alt="Description of your image">

   Output on Tableau:
   
**4. Top K Movies for users with a specific Occupation (executive/managerial, based on count)**

   Output from the program:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/f90235ae-3b10-4464-8779-3a8ec7d02ef7" width="600" alt="Description of your image">

   Output on Tableau:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/d58be3f1-87c0-419f-8731-9f148fce8e16" alt="Description of your image" width="600"><br>
   
**5. Top K Movies for each Gender (based on count)**

   Output from the program:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/e400a802-137d-427b-805a-a1f7a09aa28c" width="600" alt="Description of your image">

   Output on Tableau:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/b33c055e-24a0-4d19-9877-437e35f4c089" width="600" alt="Description of your image"><br>

   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/87ad7603-0ed1-487e-b048-d577d53c02f9" width="600" alt="Description of your image"><br>

**6. Top K Movies for each Genre (based on count)**

   Output from the program:
    
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/b8270544-5348-4fa4-ab08-48d4e431fad2" alt="Description of your image" width="600">

   Output on Tableau:
   
   <img src="https://github.com/cs535-fall23/p3-team-05/assets/142822187/af63becf-f1a8-454a-86ac-7207e3029cf8" alt="Image description" width="600"><br>

Hence, our results were verified to be correct.

**Timing**

After running the script on local machine, we were also able to execute it on the cluster. Cluster execution took much less time compared to local machine, which we enjoyed the most.

Timings for running on large dataset (ml-1m)

**Time on Local:**

* real	2m10.394s
* user	9m24.103s
* sys	   0m35.022s

**Time on cluster:**

* real    1m28.672s
* user    1m35.026s
* sys     0m5.558s

  
## Team work	

Each team member reflects on their experiences in working as a team.

### Reflection by Atharva Pargaonkar: 

In this project, I was able to understand, comprehend and apply the concepts of Dataframes in
spark as we implemented multiple dataframes on very large datasets. I enjoyed running the scripts
on cluster as it took much less time compared to local machine. I found that implementing this
project very interesting as it had similarities with MySQL where we write join table queries
to analyze data.

### Reflection by Team-mate 2:

Since Atharva and I had already worked together in a team for our other course, the communication
and collaboration required to work in a team came naturally to us. We were able to effectively
divide tasks, share ideas, and provide constructive feedback to each other. Additionally,
our previous experience helped us anticipate each other's strengths and weaknesses, allowing
us to allocate responsibilities accordingly. Atharva's experience with SQL came in handy for
this project.

Overall, the project went really well. I enjoyed working in a team and stressing together
over the issues that came up during the course of the project. It was a great learning
experience. Additionally, collaborating with different perspectives helped us to come up with
more innovative ideas and make better decisions, which is definitely one of the biggest plus
points of working in a team.

## Sources used

1. https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html
