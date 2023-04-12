# Database configuration
DB_CONFIG = {
    'driver': 'com.mysql.jdbc.Driver',
    'url': 'jdbc:mysql://your_db_host:3306/your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password'
}

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap_servers': 'your_kafka_bootstrap_servers',
    'topic': 'your_kafka_topic'
}

# S3 configuration
S3_CONFIG = {
    'access_key': 'your_aws_access_key',
    'secret_key': 'your_aws_secret_key',
    'bucket': 'your_s3_bucket'
}

# Redshift configuration
REDSHIFT_CONFIG = {
    'url': 'jdbc:redshift://your_redshift_cluster_endpoint:5439/your_db_name',
    'user': 'your_redshift_user',
    'password': 'your_redshift_password'
}
