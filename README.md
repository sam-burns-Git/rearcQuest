# Rearc Quest
## Take Home Assessment for rearc

### Part 1: AWS S3 & Data Sourcing

-> Link to my S3 bucket [s3://rearc-quest-sam](https://eu-north-1.console.aws.amazon.com/s3/buckets/rearc-quest-sam?region=eu-north-1&bucketType=general&tab=objects) <br>
-> I created the S3 bucket and created a boto3 client to access the bucket <br>
-> I created a user agent to overcome the 403 error response from the BLS URL <br>

-> **get_file_list()** uses requests and beautiful soup to scrape the file urls and append them to a list <br>
-> **upload_to_s3()** downloads these files locally and then uploads them to my S3 bucket. Uploading is done with my boto3 client I created earlier <br>
-> **sync_bls_data()** is a function that uses 'get_file_list()' to check the files that are on BLS. It then checks this against the file list in the S3 bucket, uploading any files that are not already in the S3 bucket <br>

### Part 2: APIs
-> Use requests to get the data from the API URL <br>
-> I Converted the data section of the JSON response into a JSON-formatted string  <br>
-> I then encoded the JSON string into a byte stream, preparing it for the S3 upload <br>
-> I then reset the buffer's pointyer to the beginning so all data is available for upload <br>
-> I then uploaded the data as a file object to S3 bucket <br>

### Part 3: Data Analytics 
-> **load_json_from_s3()** loads in the population.json file. It reads and decodes the JSON object. Parses it into a dictionary and then converts it into a Dataframe <br>
-> **load_csv_from_s3()** loads in the pr.data.0.Current file. It reads the csv object from the s3, accesses the body and reads the data. It then converts it into a datframe (using tab as a delimeter) <br>

-> I converted the 'Year' column in the population df to numeric <br>
-> Filter the population df to only include years from 2013 to 2028 <br>
-> Then I generated the standard dev and mean of population for these years <br>

-> Before moving on, I needed to remove any white space from all the columns names in the df <br>
-> I the grouped by Series_id and year, then got then max sum of value for each series ID <br>
-> I extracted the rows where each 'series_id' that had the max value and dropped unneccessary columns <br>
-> Printed the result 

-> I created a function to strip whitespace form the column strings and applied this to both dataframes <br>
-> use an inner join on both the 'Year' columns, bringing across the population column from population.json. <br>
-> Filter this inner join by specific series id and Q01 <br>
-> Print the result







