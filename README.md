# Spark-With-STEDI-Human-Balance-Analytics
## About
In this project, I will extract data generated by the STEDI Step Trainer sensors and the accompanying mobile application. This data will be organized and curated into a data lakehouse solution on Amazon Web Services (AWS) to enable Data Scientists to train the learning model effectively.

## Project outline
The STEDI team is developing an innovative step training device to help users improve their balance through specialized exercises. The device relies on a motion sensor that records the detected body distance during training and integrates seamlessly with a mobile application that interacts with the device's sensors. The app also utilizes the mobile phone's accelerometer to detect movement in the X, Y, and Z directions.

So far, many customers have received their step training devices, installed the mobile app, and started using them together to test their balance.Additionally, some early users have agreed to share their data to support research efforts.The STEDI team aims to use the motion sensor data to train a machine-learning model capable of accurately detecting steps in real-time.
‏               
## Datasets information & sources
The first dataset contains customer data in JSON format generated by fulfillment and the STEDI website, which includes metadata about customers and their consent to share the data with others, friends, or for research.
The second dataset consists of Step Trainer files in JSON format generated by the motion sensor, which contains metadata about the sensor and distance.
The third dataset consists of accelerometer files in JSON format generated by the mobile app, which contains metadata about timestamp, users, and detect motion in several directions.

## Configuration
#### create the S3 Gateway endpoint
aws ec2 create-vpc-endpoint --vpc-id vpc-076*********--service-name com.amazonaws.us-east-1.s3 --route-table-ids rtb-052**********
 
#### creating an IAM Service Role
aws iam create-role --role-name my-glue-service-role --assume-role-policy-document '{

    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'

#### Grant Glue Privileges on the S3 Bucket
aws iam put-role-policy --role-name my-glue-service-role --policy-name S3Access --policy-document '{

    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListObjectsInBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3::: spark-and-stedi-human-balance-w1"
            ]
        },
        {
            "Sid": "AllObjectActions",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": [
                "arn:aws:s3::: spark-and-stedi-human-balance-w1 /*"
            ]
        }
    ]
}'



## Implementing

### Athena analysis query on the Landing Zone
##### Querying the glue table customer_landing

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/customer_landing.png?raw=true)

##### Querying the glue table accelerometer_landing

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/accelerometer_landing.png?raw=true)


##### Querying the glue table step_trainer_landing 

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/step_trainer_landing.png?raw=true)



## Glue pipelines to transfer data 
### Athena analysis query on the Trusted Zone  
##### The customer landing trusted: 
Python script that utilizes Apache Spark to process and sanitize customer data collected from the website's Landing Zone. This script is designed to filter and retain only those customer records of individuals who have consented to share their data for research purposes, ensuring that only approved information is stored in the Trusted Zone.

##### Querying the glue table customer_trusted 

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/customer_trusted.png?raw=true)


##### The accelerometer_landing_trusted: 
Python script that utilizes Spark to process and sanitize accelerometer data collected from the Mobile App's Landing Zone. This script is designed to store accelerometer readings only from customers who have consented to share their data for research purposes, ensuring that only trusted data is kept in the Trusted Zone.

##### Querying the glue table accelerometer_trusted

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/accelerometer_trusted.png?raw=true)


##### The step_trainer_landing_trusted:
Python script that works with Apache Spark. Its primary function is to read the Step Trainer IoT data stream from S3 and populate a Trusted Zone Glue Table, referred to as "step_trainer_trusted." This table contains data records from Step Trainer, specifically for customers who have accelerometer data and have consented to share their information for research purposes. These customers are referred to as "customers_curated."Curated Zone.

##### Querying the glue table step_trainer_trusted

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/step_trainer_trusted.png?raw=true)



### Athena analysis query on the Curated Zone  
##### The customers_trusted_curated:
Python script that utilizes Apache Spark to sanitize customer data in the Trusted Zone. The script processes this data to create a Glue Table within the Curated Zone. This new table exclusively includes customers who possess accelerometer data and have consented to share their information for research purposes. This resultant dataset is referred to as the "customers curated."

##### Querying the glue table customer_trusted

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/customer_trusted.png?raw=true)


##### Machine_learning_curated: 
Python script utilizes Spark to generate an aggregated table named "machine_learning_curated." It compiles Step Trainer readings along with corresponding accelerometer data, ensuring that only information from customers who have consented to share their data is included. The resulting table is then populated in a Glue table for further use in machine-learning applications.

#####  Querying the glue table machine_learning_trusted

![image alt](https://github.com/WaadAlharbi/Spark-With-STEDI-Human-Balance-Analytics/blob/main/Screenshots/machine_learning_trusted.png?raw=true)








        




