import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    print(f"Source Bucket: {source_bucket}, Object Key: {object_key}")
    
    target_bucket = 'cleaned-data-zone-csv-bucket'
    target_file_name = object_key[:-5]  # File name without JSON suffix
    print(f"Target File Name: {target_file_name}")

    # Wait for the object to exist
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    # Get the object from S3
    try:
        response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
        data = response['Body'].read().decode('utf-8')
        print("Successfully retrieved and decoded data from S3.")
    except Exception as e:
        print(f"Error retrieving object from S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error retrieving object from S3')
        }
    
    # Load JSON data
    try:
        data = json.loads(data)
        print("Successfully loaded JSON data.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid JSON format')
        }
    
    # Check for 'data' and 'products'
    if 'data' not in data or 'products' not in data['data']:
        print("Invalid data structure: 'data' or 'products' key missing.")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid data structure')
        }
    
    # Convert products to DataFrame
    products = data['data']['products']
    df = pd.DataFrame(products)
    
    # Debug: Print DataFrame columns
    print("Columns:", df.columns.tolist())

    # Specify columns to select
    selected_columns = [
        'asin', 'product_title', 'product_price', 'product_original_price', 
        'currency', 'product_star_rating', 'product_num_ratings', 
        'product_url', 'product_photo', 'product_num_offers', 
        'is_best_seller', 'is_prime', 'sales_volume', 'delivery', 
        'has_variations'
    ]
    
    # Check for missing columns
    missing_columns = [col for col in selected_columns if col not in df.columns]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
        return {
            'statusCode': 400,
            'body': json.dumps(f'Missing columns: {missing_columns}')
        }
    
    # Select specified columns
    df = df[selected_columns]

    # Check if DataFrame is empty before processing
    if df.empty:
        print("DataFrame is empty after filtering selected columns.")
        return {
            'statusCode': 400,
            'body': json.dumps('No data available to process')
        }

    # Clean price columns and convert to numeric
    for price_col in ['product_price', 'product_original_price']:
        df[price_col] = df[price_col].replace({'\$': '', ',': ''}, regex=True)
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce').fillna(0)

    print("Processed DataFrame:")
    print(df)
    
    # CSV conversion
    csv_data = df.to_csv(index=False)
    
    # Upload CSV to the final S3 bucket
    try:
        s3_client.put_object(Bucket=target_bucket, Key=f'{target_file_name}.csv', Body=csv_data)
        print("CSV uploaded successfully.")
    except Exception as e:
        print(f"Error uploading CSV to S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error uploading CSV to S3')
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('.csv conversion and S3 upload completed!')
    }
