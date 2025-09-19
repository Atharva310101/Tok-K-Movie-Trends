'''
CS535 : LARGE SCALE DATA ANALYSIS
TEAM PROJECT : TEAM 05
@authors: Atharva Pargaonkar , Shrutee Dwa
This program finds the top 'k' movies for six different trends for MovieLens 1M dataset.

'''

#Importing the required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, explode, split, rank, avg, count, format_number    
from pyspark.sql.window import Window
import sys  
from pyspark.sql.functions import from_unixtime, month, udf
from pyspark.sql.types import StringType

# Initializing the spark session
def init_spark_session():
    return SparkSession.builder.appName("MovieLensDataAnalysis").getOrCreate()

# Loading the datasets using spark dataframes
def load_datasets(spark, data_path):
    ratings_df = spark.read.csv(data_path + "/ratings.dat", sep="::", header=False, inferSchema=True)
    users_df = spark.read.csv(data_path + "/users.dat", sep="::", header=False, inferSchema=True)
    movies_df = spark.read.csv(data_path + "/movies.dat", sep="::", header=False, inferSchema=True)
    return ratings_df, users_df, movies_df

# Renaming the columns of the dataframes because the default column names do not have headers
# withColumnRenamed() function is used to rename the columns 
def rename_columns(ratings_df, users_df, movies_df):
    ratings_df = ratings_df.withColumnRenamed("_c0", "UserID").withColumnRenamed("_c1", "MovieID").withColumnRenamed("_c2", "Rating").withColumnRenamed("_c3", "Timestamp")
    users_df = users_df.withColumnRenamed("_c0", "UserID").withColumnRenamed("_c1", "Gender").withColumnRenamed("_c2", "Age").withColumnRenamed("_c3", "Occupation")
    movies_df = movies_df.withColumnRenamed("_c0", "MovieID").withColumnRenamed("_c1", "Title").withColumnRenamed("_c2", "Genres")
    return ratings_df, users_df, movies_df

#--------------------------------------------------------------
# Defining functions for each trend to be analyzed
#--------------------------------------------------------------

#----------------------------------------------------------------------
# Trend 1 : Top K popular movies of all time based on average rating
#----------------------------------------------------------------------

# This function takes the ratings_df and movies_df as input and outputs the top k movies of all time
def analyze_top_movies_all_time(ratings_df, movies_df, k, output_file):
    
    joined_df = ratings_df.join(movies_df, "MovieID", "inner") \
        .select(
            ratings_df["MovieID"],
            movies_df["Title"],
            ratings_df["Rating"]
        )   # Joining the ratings_df and movies_df to get the movie title and rating for each movie
    
    # Calculating the average rating and popularity of each movie
    average_ratings_df = joined_df.groupBy("MovieID", "Title") \
        .agg(
            avg("Rating").alias("AverageRating"),
            count("Rating").alias("Popularity (Movie rated at least 500 times)")
        )
    
    # Formating it show two decimal places
    average_ratings_df = average_ratings_df.withColumn(
        "AverageRating", format_number("AverageRating", 2)
    )
    
    popularity_threshold = 500  # Minimum number of ratings for a movie to be considered popular

    # Filter movies with popularity above the threshold
    filtered_movies_df = average_ratings_df.filter(col("Popularity (Movie rated at least 500 times)") > popularity_threshold)

    # Get the top k most popular movies of all time sorted by average rating, popularity, and movie title
    top_movies_all_time = filtered_movies_df.orderBy(col("AverageRating").desc(), col("Popularity (Movie rated at least 500 times)").desc(), col("Title").asc()).limit(k)
    print(f"Top {k} Most Popular Movies of All Time are as follows: \n")
    top_movies_all_time.show(top_movies_all_time.count(), truncate=False)
    print("\n")
    top_movies_all_time.coalesce(1).write.mode("overwrite").csv(output_file, header=True) # Writing the output to a csv file

#----------------------------------------------------------------------------
# Trend 2 : Top K popular movies for a particular age group based on count
#---------------------------------------------------------------------------- 

# Defining a dictionary for all the age groups as per the Dataset README.md file to get the age range in the output
age_range_mapping = {
    1: "Under 18",
    18: "18-24",
    25: "25-34",
    35: "35-44",
    45: "45-49",
    50: "50-55",
    56: "56+"
}

# This function takes the joined_df, target_age_group, and k as input and outputs the top k movies for the specified age group
def analyze_top_movies_by_age_group(joined_df, target_age_group, k, output_file):
    
    # Filter the dataset to include only the target age group
    filtered_df = joined_df.filter(users_df["Age"] == target_age_group)

    # Calculate the popularity of each movie by counting ratings
    popular_movies_age_group_df = filtered_df.groupBy("MovieID", "Title").count()

    # Get the top k most popular movies sorted by count and movie title for the specified age group
    top_movies_age_group = popular_movies_age_group_df.orderBy(col("count").desc(), col("Title").asc()).limit(k)

    # Displaying the results with the age range instead of numerical code
    age_range = age_range_mapping.get(target_age_group, f"Unknown Age Range {target_age_group}")
    print(f"Top {k} Movies for Age Range '{age_range}' are as follows: \n")
    top_movies_age_group.show(top_movies_age_group.count(), truncate=False)
    print("\n")
    top_movies_age_group.coalesce(1).write.mode("overwrite").csv(output_file, header=True) # Writing the output to a csv file

#------------------------------------------------------------------------
# Trend 3 : Top K Popular movies for a particular season based on count
#------------------------------------------------------------------------

# User defined function to convert month to season based on the timestamp
def month_to_season(month):
    if month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    elif month in [9, 10, 11]:
        return 'Autumn'
    else:  # December, January, February
        return 'Winter'

# Registering user defined function
season_udf = udf(month_to_season, StringType())

# This function takes the joined_df, target_season, and k as input and outputs the top k movies for the specified season
def analyze_top_movies_by_season(joined_df, target_season, k, output_file):
    seasonal_df = joined_df.withColumn("Season", season_udf(month(from_unixtime(joined_df["Timestamp"])))) 
    # from_unixtime converts the timestamp to date format and month extracts the month from the date format
    # example : from_unixtime(978300760) = 2001-01-01 01:00:00

    filtered_seasonal_df = seasonal_df.filter(seasonal_df["Season"] == target_season)   # Filtering the dataset to include only the target season
    popular_movies_season_df = filtered_seasonal_df.groupBy("MovieID", "Title").count() # Calculating the popularity of each movie by counting ratings
    
    # Get the top k most popular movies sorted by count and movie title sorted by count and movie title for the specified season
    top_movies_season_df = popular_movies_season_df.orderBy(col("count").desc()).limit(k) 
    print(f"Top {k} Movies for {target_season.capitalize()} Season are as follows:\n")
    top_movies_season_df.show(top_movies_season_df.count(), truncate=False)
    print("\n")
    top_movies_season_df.coalesce(1).write.mode("overwrite").csv(output_file, header=True)  # Writing the output to a csv file

#---------------------------------------------------------------------------
# Trend 4 : Top K popular movies for a particular occupation based on count
#---------------------------------------------------------------------------

# Defining a dictionary for all the occupations as per the Dataset README.md file to get the occupation name in the output
occupation_mapping = {
        0: "other or not specified",
        1: "academic/educator",
        2: "artist",
        3: "clerical/admin",
        4: "college/grad student",
        5: "customer service",
        6: "doctor/health care",
        7: "executive/managerial",
        8: "farmer",
        9: "homemaker",
        10: "K-12 student",
        11: "lawyer",
        12: "programmer",
        13: "retired",
        14: "sales/marketing",
        15: "scientist",
        16: "self-employed",
        17: "technician/engineer",
        18: "tradesman/craftsman",
        19: "unemployed",
        20: "writer"
    }

# This function takes the joined_df, target_occupation, and k as input and outputs the top k movies for the specified occupation
def analyze_top_movies_by_occupation(joined_df, target_occupation,k, output_file):
    # Extract the occupation name from the mapping
    occupation_name = occupation_mapping.get(target_occupation, f"Unknown Occupation {target_occupation}")

    # Filter the dataset to include only the target occupation
    filtered_df = joined_df.filter(users_df["Occupation"] == target_occupation)

    # Get the top k most popular movies sorted by count and movie title for the specified occupation
    popular_movies_occupation_df = filtered_df.groupBy("MovieID", "Title").count()
    top_movies_occupation = popular_movies_occupation_df.orderBy(col("count").desc(), col("Title").asc()).limit(k)

    print(f"Top {k} Movies for Users with Occupation '{occupation_name}' are as follows: \n")
    top_movies_occupation.show(top_movies_occupation.count(), truncate=False)
    print("\n")
    top_movies_occupation.coalesce(1).write.mode("overwrite").csv(output_file, header=True)     # Writing the output to a csv file

#------------------------------------------------------------------
# Trend 5 : Top K popular movies for each gender based on count
#------------------------------------------------------------------

# Defining target variables for each gender as per the Dataset README.md file
target_gender_male = 'M'
target_gender_female = 'F'

# This function takes the joined_df and k as input and outputs the top k movies for both Male and Female users separately
def analyze_top_movies_by_gender(joined_df,k, output_file1, output_file2):

    # Filtering the dataset to include both genders
    filtered_df_male = joined_df.filter(users_df["Gender"] == target_gender_male)
    filtered_df_female = joined_df.filter(users_df["Gender"] == target_gender_female)

    # Get the top k most popular movies sorted by count and movie title for both genders
    popular_movies_male_df = filtered_df_male.groupBy("MovieID", "Title").count()
    popular_movies_female_df = filtered_df_female.groupBy("MovieID", "Title").count()
    top_movies_male = popular_movies_male_df.orderBy(col("count").desc(), col("Title").asc()).limit(k)
    top_movies_female = popular_movies_female_df.orderBy(col("count").desc(), col("Title").asc()).limit(k)


    # Printing the results for Male
    print(f"Top {k} Movies for Male Users are as follows: \n")
    top_movies_male.show(top_movies_male.count(), truncate=False)
    print("\n")

    # Printing the results for Female
    print(f"Top {k} Movies for Female Users are as follows: \n")
    top_movies_female.show(top_movies_female.count(), truncate=False)
    print("\n")

    # Writing the output to a csv file for both Male and Female separately
    top_movies_male.coalesce(1).write.mode("overwrite").csv(output_file1, header=True)
    top_movies_female.coalesce(1).write.mode("overwrite").csv(output_file2, header=True)


#---------------------------------------------------------------
# Trend 6 : Top K popular movies for each genre based on count
#---------------------------------------------------------------

# Defining a list of all the genres as per the Dataset README.md file to get the genre name in the output
genres = [
    'Action', 'Adventure', 'Animation', 'Children\'s', 'Comedy', 'Crime', 'Documentary',
    'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi',
    'Thriller', 'War', 'Western'
]

# This function takes the joined_df, target_genre, and k as input and outputs the top k movies for the specified genre
def analyze_top_movies_by_genre(joined_df, target_genre, k, output_file):
    # withColumn() function is used to create a new column 'Genre' by splitting the 'Genres' column
    # explode() function is used to split the 'Genre' column into multiple rows for each genre
    # split() function is used to split the 'Genres' column into multiple columns based on the delimiter '|'

    exploded_genres_df = joined_df.withColumn("Genre", explode(split(col("Genres"), "\\|")))

    # Filter the dataset to include only the target genre
    popularity_by_genre_df = exploded_genres_df.groupBy("Genre", "MovieID", "Title").count()

    # Get the top k most popular movies sorted by count and movie title for the specified genre
    # window() function is used to create a window specification for the rank function
    # window_spec is used to partition the data by genre and order it by count in descending order
    # rank() function is used to assign a rank to each movie based on the count    
    window_spec = Window.partitionBy("Genre").orderBy(col("count").desc())
    top_movies_by_genre_df = popularity_by_genre_df.withColumn("rank", rank().over(window_spec)).filter(col("rank") <= k).select("rank", "Genre", "MovieID", "Title", "count")
    
    # Displaying the results with the genre name instead of numerical code
    genre_dataframes = {}

    # for loop to create a dictionary of dataframes for each genre
    for genre in genres:
        genre_df = top_movies_by_genre_df.filter(col("Genre") == genre)
        genre_dataframes[genre] = genre_df
        # Writing the output to a csv file
        genre_df.coalesce(1).write.mode("overwrite").csv(output_file+"_"+genre, header=True)
    
    # Printing the results for each genre
    for genre, genre_df in genre_dataframes.items():
        print(f"Top {k} movies for {genre} genre:")
        genre_df.show(genre_df.count(), truncate=False)
        print("\n")
            
#--------------------------------------------------------------
# Main function to call all the functions defined above
#--------------------------------------------------------------

if __name__ == "__main__":

    # Check if the number of arguments is correct
    if (len(sys.argv) != 4):
        print("Usage: Top_k_trends.py <value for k> <input folder> <output folder>")
        sys.exit(1)

    # Reading the command line arguments for k, input_path, and output_path
    k = int(sys.argv[1])
    input_path=sys.argv[2]  
    output_path=sys.argv[3]

    # Define output file names for each trend to be analyzed
    output_file_all_time = "/top_movies_all_time_output"
    output_file_age_group = "/top_movies_age_group_output"
    output_file_season = "/top_movies_season_output"
    output_file_occupation = "/top_movies_occupation_output"
    output_file_male = "/top_movies_male_output"
    output_file_female = "/top_movies_female_output"
    output_file_genre = "/top_movies_genre_output"
    
    spark = init_spark_session()     # Function call to initialize the spark session

    # Function call to load the datasets using spark dataframes
    ratings_df, users_df, movies_df = load_datasets(spark, input_path)
    ratings_df, users_df, movies_df = rename_columns(ratings_df, users_df, movies_df) 	
    
    # Joining the three dataframes to get the required columns. Inner join is used to get only the common rows
    joined_df = ratings_df.join(users_df, "UserID", "inner").join(movies_df, "MovieID", "inner") \
        .select(
            ratings_df["UserID"],
            ratings_df["MovieID"],
            ratings_df["Rating"],
            ratings_df["Timestamp"],
            users_df["Occupation"],
            movies_df["Title"],
            movies_df["Genres"]
        )

    # Printing the trends to be analyzed for the user
    print("\n We will be analyzing the following trends: ")
    print("1. Top K Movies of all time")
    print("2. Top K Movies for a specific Age Group")
    print("3. Top K Movies for a specific Season")
    print("4. Top K Movies for users with a specific Occupation")
    print("5. Top K Movies for each Gender")
    print("6. Top K Movies for each Genre")
    
    # Function calls to analyze the top k movies for each trend 
    analyze_top_movies_all_time(ratings_df, movies_df, k, output_path + output_file_all_time)
    analyze_top_movies_by_age_group(joined_df, 18, k, output_path + output_file_age_group)
    analyze_top_movies_by_season(joined_df, 'Spring', k, output_path + output_file_season)
    analyze_top_movies_by_occupation(joined_df, 7, k, output_path + output_file_occupation)
    analyze_top_movies_by_gender(joined_df, k, output_path + output_file_male, output_path + output_file_female)
    analyze_top_movies_by_genre(joined_df, genres, k, output_path + output_file_genre)
   
    print("Analysis Completed and Output Files Generated!")
