from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CSV Transformation") \
    .getOrCreate()


# Read the CSV file
input_file = "foodhub_order.csv"  # Replace with your CSV file path
df = spark.read.csv(input_file, header=True, inferSchema=True)
print(df.show(5))

# Group by restaurant name and count the number of orders
top_restaurants = df.groupBy("restaurant_name").agg(count("restaurant_name").alias("order_count"))

# Sort the restaurants by the number of orders in descending order
top_restaurants_sorted = top_restaurants.orderBy(col("order_count").desc())

# Display the top five restaurants
print("Top Five Restaurants in terms of the number of orders received:")
top_restaurants_sorted.show(5)

# Filter for weekend orders
weekend_orders = df.filter(col("day_of_the_week") == "Weekend")

# Group by cuisine type and count the number of orders
popular_cuisine = weekend_orders.groupBy("cuisine_type").agg(count("cuisine_type").alias("order_count"))

# Sort by the number of orders in descending order
popular_cuisine_sorted = popular_cuisine.orderBy(col("order_count").desc())

# Display the most popular cuisine on weekends
print("The most popular cuisine on weekends is:")
popular_cuisine_sorted.show(1)

# Group by restaurant name and calculate total rating count and average rating
restaurant_ratings = df.groupBy("restaurant_name").agg(
    count("rating").alias("rating_count"),
    avg("rating").alias("average_rating")
)

# Filter restaurants with rating count > 50 and average rating > 4
filtered_restaurants = restaurant_ratings.filter(
    (col("rating_count") > 50) & (col("average_rating") > 4)
)

# Display the list of restaurants
print("Restaurants with a rating count of more than 50 and an average rating greater than 4:")
filtered_restaurants.select("restaurant_name").show()

# Stop the SparkSession
spark.stop()



