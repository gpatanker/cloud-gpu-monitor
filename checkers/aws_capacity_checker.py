#!/usr/bin/env python3
import boto3
import json
import time
import os
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Set to True to enable more detailed debug information
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('aws_capacity_checker')

# MySQL database configuration
# DB_CONFIG = {
#     'host': os.getenv('MYSQL_HOST', 'localhost'),
#     'user': os.getenv('MYSQL_USER', 'root'),
#     'password': os.getenv('MYSQL_PASSWORD', ''),
#     'database': os.getenv('MYSQL_DATABASE', 'cloud_capacity')
# }

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),  # This should be the full RDS endpoint
    'user': os.getenv('MYSQL_USER'),  # Your RDS master username
    'password': os.getenv('MYSQL_PASSWORD').strip("'"),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': int(os.getenv('MYSQL_PORT', 3306))
}


def log_debug(message):
    """Helper function for debug logging"""
    if DEBUG:
        logger.debug(message)

def log_info(message):
    """Helper function for info logging"""
    logger.info(message)

def log_error(message):
    """Helper function for error logging"""
    logger.error(message)

def verify_database_table():
    """Verify that the aws_gpu_instances table exists, creating it if needed"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if the table exists
        cursor.execute("SHOW TABLES LIKE 'aws_gpu_instances'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            log_info("Creating aws_gpu_instances table...")
            # Create the table with on_demand_price column
            create_table_sql = """
            CREATE TABLE `aws_gpu_instances` (
              `id` int NOT NULL AUTO_INCREMENT,
              `instance_type` varchar(255) NOT NULL,
              `region` varchar(255) NOT NULL,
              `available` tinyint(1) DEFAULT '0',
              `offered_in_region` tinyint(1) DEFAULT '0',
              `on_demand_price` decimal(10,4) DEFAULT NULL,
              `spot_available` tinyint(1) DEFAULT '0',
              `spot_price` decimal(10,4) DEFAULT NULL,
              `gpu_type` varchar(255) DEFAULT NULL,
              `gpu_memory` varchar(255) DEFAULT NULL,
              `gpu_count` int DEFAULT '0',
              `check_time` datetime DEFAULT NULL,
              `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              KEY `region_instance_idx` (`region`,`instance_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_table_sql)
            conn.commit()
            log_info("Table created successfully")
        else:
            # Check if on_demand_price column exists, add it if not
            cursor.execute("SHOW COLUMNS FROM aws_gpu_instances LIKE 'on_demand_price'")
            column_exists = cursor.fetchone() is not None
            
            if not column_exists:
                log_info("Adding on_demand_price column to aws_gpu_instances table...")
                cursor.execute("ALTER TABLE aws_gpu_instances ADD COLUMN on_demand_price decimal(10,4) DEFAULT NULL AFTER offered_in_region")
                conn.commit()
                log_info("Column added successfully")
        
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as e:
        log_error(f"Database error: {e}")
        return False

def get_all_regions():
    """Get all available AWS regions dynamically"""
    try:
        # Create a client using the default region
        ec2_client = boto3.client('ec2', region_name='us-east-1')
        
        # Get all available regions
        response = ec2_client.describe_regions()
        regions = [region['RegionName'] for region in response['Regions']]
        
        log_debug(f"Found {len(regions)} AWS regions")
        return regions
    except Exception as e:
        log_error(f"Error getting regions: {e}")
        # Fallback to a minimal set of major regions
        fallback_regions = [
            'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
            'eu-west-1', 'eu-central-1', 'ap-northeast-1', 'ap-southeast-1'
        ]
        log_info(f"Using fallback list of {len(fallback_regions)} regions")
        return fallback_regions

def get_gpu_instance_types(ec2_client):
    """Dynamically discover GPU instance types in a region"""
    try:
        # Initialize list to hold GPU instance types
        gpu_instances = []
        
        # Create a paginator for the describe_instance_types API
        paginator = ec2_client.get_paginator('describe_instance_types')
        
        # Paginate through all instance types
        for page in paginator.paginate():
            for instance_type in page['InstanceTypes']:
                # Check if the instance has GPUs
                if 'GpuInfo' in instance_type:
                    gpu_info = instance_type['GpuInfo']
                    if 'Gpus' in gpu_info and len(gpu_info['Gpus']) > 0:
                        gpu_instances.append(instance_type['InstanceType'])
        
        log_debug(f"Found {len(gpu_instances)} GPU instance types")
        return gpu_instances
    except Exception as e:
        log_error(f"Error getting GPU instance types in {ec2_client.meta.region_name}: {e}")
        return []

def standardize_gpu_type_naming(gpu_type):
    """
    Standardize GPU type naming dynamically to match Azure's format (VENDOR MODEL)
    """
    if gpu_type is None or gpu_type == 'Unknown':
        return 'Unknown GPU'
        
    # Handle normalized case and remove any extra spaces
    gpu_type = gpu_type.strip()
    
    # Already has a vendor prefix
    if any(gpu_type.startswith(vendor) for vendor in ['NVIDIA', 'AMD', 'Intel', 'Habana']):
        return gpu_type
    
    # Process hyphenated formats like "nvidia-t4"
    if '-' in gpu_type:
        parts = gpu_type.split('-')
        # Detect vendor in hyphenated string
        if parts[0].lower() in ['nvidia', 'amd', 'intel', 'habana']:
            vendor = parts[0].upper() if parts[0].lower() == 'amd' else parts[0].capitalize()
            # Special case for Tesla
            if len(parts) > 2 and parts[1].lower() == 'tesla':
                return f"{vendor} Tesla {parts[2].upper()}"
            else:
                # Join the rest and uppercase model numbers
                model = ''.join(part.upper() if part.isalnum() and not part.isalpha() else part.capitalize() 
                               for part in parts[1:])
                return f"{vendor} {model}"
    
    # Known GPU families mapping - EXTENDED with latest models
    vendors = {
        'A100': 'NVIDIA',
        'A10': 'NVIDIA',
        'A10G': 'NVIDIA',
        'A40': 'NVIDIA',
        'H100': 'NVIDIA',
        'H200': 'NVIDIA',
        'GH200': 'NVIDIA',
        'GB200': 'NVIDIA',
        'T4': 'NVIDIA',
        'V100': 'NVIDIA Tesla',
        'P100': 'NVIDIA Tesla',
        'K80': 'NVIDIA Tesla',
        'M60': 'NVIDIA',
        'L4': 'NVIDIA',
        'Gaudi': 'Habana',
        'Radeon': 'AMD',
        'MI': 'AMD Radeon Instinct'
    }
    
    # Try to identify vendor by GPU model prefix
    for model_prefix, vendor in vendors.items():
        if gpu_type.upper().startswith(model_prefix) or model_prefix in gpu_type.upper():
            # Format model name - uppercase letters with numbers
            model = ''.join(c.upper() if i > 0 and c.isdigit() else c 
                          for i, c in enumerate(gpu_type))
            return f"{vendor} {model}"
    
    # Default to NVIDIA for unrecognized GPUs
    return f"NVIDIA {gpu_type}"

def get_gpu_info(instance_type, ec2_client):
    """Get detailed GPU information for an instance type directly from EC2 API"""
    try:
        # Get instance type details
        response = ec2_client.describe_instance_types(InstanceTypes=[instance_type])
        
        if not response['InstanceTypes']:
            return {'gpu_type': 'Unknown', 'gpu_memory': 'Unknown', 'gpu_count': 0}
        
        instance_info = response['InstanceTypes'][0]
        
        # Default values
        gpu_info = {
            'gpu_type': 'Unknown',
            'gpu_memory': 'Unknown',
            'gpu_count': 0
        }
        
        # Extract GPU info if available
        if 'GpuInfo' in instance_info:
            gpu_details = instance_info['GpuInfo']
            if 'Gpus' in gpu_details and gpu_details['Gpus']:
                gpus = gpu_details['Gpus']
                
                # Get GPU count (total across all GPU types)
                gpu_info['gpu_count'] = sum(gpu.get('Count', 1) for gpu in gpus)
                
                # Get GPU type and memory from the first GPU
                if gpus:
                    first_gpu = gpus[0]
                    gpu_info['gpu_type'] = standardize_gpu_type_naming(first_gpu.get('Name', 'Unknown GPU'))
                    
                    # Memory is in MB, convert to GB
                    if 'MemoryInfo' in first_gpu and 'SizeInMiB' in first_gpu['MemoryInfo']:
                        memory_mb = first_gpu['MemoryInfo']['SizeInMiB']
                        memory_gb = memory_mb / 1024
                        gpu_info['gpu_memory'] = f"{int(memory_gb)} GB"
        
        # Fallback matching based on instance type pattern if needed
        if gpu_info['gpu_type'] == 'Unknown' or gpu_info['gpu_count'] == 0:
            instance_family = instance_type.split('.')[0]
            
            if instance_family == 'p3':
                gpu_info['gpu_type'] = standardize_gpu_type_naming('Tesla V100')
                if 'p3.2xlarge' in instance_type:
                    gpu_info['gpu_count'] = 1
                    gpu_info['gpu_memory'] = '16 GB'
                elif 'p3.8xlarge' in instance_type:
                    gpu_info['gpu_count'] = 4
                    gpu_info['gpu_memory'] = '16 GB'
                elif 'p3.16xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '16 GB'
                elif 'p3dn.24xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '32 GB'
            
            elif instance_family == 'p4d' or instance_family == 'p4de':
                gpu_info['gpu_type'] = standardize_gpu_type_naming('A100')
                if 'p4d.24xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '40 GB'
                elif 'p4de.24xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '80 GB'
            
            elif instance_family == 'p5':
                gpu_info['gpu_type'] = standardize_gpu_type_naming('H100')
                if 'p5.48xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '80 GB'

            # Add P5e instances with H100
            elif instance_family == 'p5e':
                gpu_info['gpu_type'] = standardize_gpu_type_naming('H100')
                if 'p5e.48xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '80 GB'
            
            # Add new P6 instances with GH200 GPUs
            elif instance_family == 'p6':
                gpu_info['gpu_type'] = standardize_gpu_type_naming('GH200')
                if 'p6.48xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '96 GB'
                elif 'p6g.48xlarge' in instance_type:  # GH200 Grace Hopper variant
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '96 GB'
                    
            # Add new P7 instances with GB200 GPUs
            elif instance_family == 'p7':
                gpu_info['gpu_type'] = standardize_gpu_type_naming('GB200')
                if 'p7.48xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '96 GB'
                    
            # Add new DL2 instances with H200 GPUs
            elif instance_family == 'dl2':
                gpu_info['gpu_type'] = standardize_gpu_type_naming('H200')
                if 'dl2.24xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    gpu_info['gpu_memory'] = '80 GB'
            
            elif instance_family.startswith('g4'):
                gpu_info['gpu_type'] = standardize_gpu_type_naming('T4')
                gpu_info['gpu_memory'] = '16 GB'
                if 'g4dn.xlarge' in instance_type or 'g4dn.2xlarge' in instance_type or 'g4dn.4xlarge' in instance_type or 'g4dn.8xlarge' in instance_type or 'g4dn.16xlarge' in instance_type:
                    gpu_info['gpu_count'] = 1
                elif 'g4dn.12xlarge' in instance_type:
                    gpu_info['gpu_count'] = 4
                elif 'g4dn.metal' in instance_type:
                    gpu_info['gpu_count'] = 8
            
            elif instance_family.startswith('g5'):
                if 'g5g' in instance_type:
                    gpu_info['gpu_type'] = 'AWS Graviton (ARM)'
                    gpu_info['gpu_count'] = 1
                else:
                    gpu_info['gpu_type'] = standardize_gpu_type_naming('A10G')
                    gpu_info['gpu_memory'] = '24 GB'
                    if any(x in instance_type for x in ['g5.xlarge', 'g5.2xlarge', 'g5.4xlarge', 'g5.8xlarge', 'g5.16xlarge']):
                        gpu_info['gpu_count'] = 1
                    elif any(x in instance_type for x in ['g5.12xlarge', 'g5.24xlarge']):
                        gpu_info['gpu_count'] = 4
                    elif 'g5.48xlarge' in instance_type:
                        gpu_info['gpu_count'] = 8
            
            elif instance_family.startswith('g6'):
                gpu_info['gpu_type'] = standardize_gpu_type_naming('L4')
                gpu_info['gpu_memory'] = '24 GB'
                if any(x in instance_type for x in ['g6.xlarge', 'g6.2xlarge', 'g6.4xlarge', 'g6.8xlarge', 'g6.16xlarge']):
                    gpu_info['gpu_count'] = 1
                elif any(x in instance_type for x in ['g6.12xlarge', 'g6.24xlarge']):
                    gpu_info['gpu_count'] = 4
                elif 'g6.48xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
                    
            # Add newly added G6e instances with A10G
            elif instance_family.startswith('g6e'):
                gpu_info['gpu_type'] = standardize_gpu_type_naming('A10G')
                gpu_info['gpu_memory'] = '24 GB'
                if any(x in instance_type for x in ['g6e.xlarge', 'g6e.2xlarge', 'g6e.4xlarge', 'g6e.8xlarge', 'g6e.16xlarge']):
                    gpu_info['gpu_count'] = 1
                elif any(x in instance_type for x in ['g6e.12xlarge', 'g6e.24xlarge']):
                    gpu_info['gpu_count'] = 4
                elif 'g6e.48xlarge' in instance_type:
                    gpu_info['gpu_count'] = 8
        
        return gpu_info
    
    except Exception as e:
        log_error(f"Error getting GPU info for {instance_type}: {e}")
        return {'gpu_type': 'Unknown', 'gpu_memory': 'Unknown', 'gpu_count': 0}

def get_spot_price(ec2_client, instance_type):
    """Get current spot price for an instance type in a region"""
    try:
        # Get current spot prices
        response = ec2_client.describe_spot_price_history(
            InstanceTypes=[instance_type],
            ProductDescriptions=['Linux/UNIX'],
            MaxResults=1
        )
        
        # Return the price if available
        if response['SpotPriceHistory']:
            return float(response['SpotPriceHistory'][0]['SpotPrice'])
        else:
            return None
    except Exception as e:
        log_error(f"Error getting spot price for {instance_type} in {ec2_client.meta.region_name}: {e}")
        return None

def get_on_demand_price(pricing_client, instance_type, region):
    """Get on-demand price for an instance type in a region"""
    try:
        # Get the on-demand price
        response = pricing_client.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': get_region_name(region)},
                {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
                {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'}
            ],
            MaxResults=100
        )
        
        if response['PriceList']:
            # Parse the price list (returns JSON as string)
            price_list = json.loads(response['PriceList'][0])
            
            # Extract the price from the terms
            terms = price_list['terms']['OnDemand']
            for term_id in terms:
                price_dimensions = terms[term_id]['priceDimensions']
                for dimension_id in price_dimensions:
                    price_dimension = price_dimensions[dimension_id]
                    price = float(price_dimension['pricePerUnit']['USD'])
                    return price
        
        return None
    except Exception as e:
        log_debug(f"Error getting on-demand price for {instance_type} in {region}: {e}")
        return None

def get_region_name(region_code):
    """Convert region code to the full region name used in the pricing API"""
    region_names = {
        'us-east-1': 'US East (N. Virginia)',
        'us-east-2': 'US East (Ohio)',
        'us-west-1': 'US West (N. California)',
        'us-west-2': 'US West (Oregon)',
        'af-south-1': 'Africa (Cape Town)',
        'ap-east-1': 'Asia Pacific (Hong Kong)',
        'ap-south-1': 'Asia Pacific (Mumbai)',
        'ap-northeast-1': 'Asia Pacific (Tokyo)',
        'ap-northeast-2': 'Asia Pacific (Seoul)',
        'ap-northeast-3': 'Asia Pacific (Osaka)',
        'ap-southeast-1': 'Asia Pacific (Singapore)',
        'ap-southeast-2': 'Asia Pacific (Sydney)',
        'ap-southeast-3': 'Asia Pacific (Jakarta)',
        'ca-central-1': 'Canada (Central)',
        'eu-central-1': 'EU (Frankfurt)',
        'eu-west-1': 'EU (Ireland)',
        'eu-west-2': 'EU (London)',
        'eu-west-3': 'EU (Paris)',
        'eu-north-1': 'EU (Stockholm)',
        'eu-south-1': 'EU (Milan)',
        'me-south-1': 'Middle East (Bahrain)',
        'sa-east-1': 'South America (Sao Paulo)'
    }
    return region_names.get(region_code, region_code)

def check_availability(ec2_client, instance_type):
    """Check if an instance type is available in a region using spot capacity"""
    try:
        # Get a list of availability zones in the region
        response = ec2_client.describe_availability_zones()
        availability_zones = [az['ZoneName'] for az in response['AvailabilityZones'] if az['State'] == 'available']
        
        # Try to find at least one AZ with capacity
        for az in availability_zones:
            try:
                # Check if we can get a spot price for the instance type in this AZ
                response = ec2_client.describe_spot_price_history(
                    InstanceTypes=[instance_type],
                    ProductDescriptions=['Linux/UNIX'],
                    AvailabilityZone=az,
                    MaxResults=1
                )
                
                if response['SpotPriceHistory']:
                    return True
            except ClientError as e:
                # If the error is not about capacity, re-raise it
                if 'capacity' not in str(e).lower():
                    raise
        
        # If we get here, there's no capacity in any AZ
        return False
    except Exception as e:
        log_error(f"Error checking availability for {instance_type} in {ec2_client.meta.region_name}: {e}")
        return False

def check_gpu_availability_for_region(region):
    """Check GPU availability for a specific region using optimized approach"""
    results = []
    
    try:
        # Create EC2 client for this region using credentials from .env
        ec2_client = boto3.client(
            'ec2', 
            region_name=region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        
        # Create pricing client in us-east-1 (the only region that supports the pricing API)
        pricing_client = boto3.client(
            'pricing', 
            region_name='us-east-1',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        
        # Get all GPU instance types available in this region
        gpu_instance_types = get_gpu_instance_types(ec2_client)
        
        if not gpu_instance_types:
            log_info(f"No GPU instance types found in {region}")
            return []
        
        log_info(f"Found {len(gpu_instance_types)} GPU instance types in {region}")

        # Add the specific latest GPU instance types if they're not already in the list
        latest_instance_types = [
            # A100 instances
            'p4d.24xlarge',    # A100 40GB
            'p4de.24xlarge',   # A100 80GB
            
            # H100 instances
            'p5.48xlarge',     # H100 80GB
            'p5e.48xlarge',    # H100 variants
            
            # H200 instances
            'dl2.24xlarge',    # H200 instances
            
            # GH200 instances
            'p6.48xlarge',     # GH200
            'p6g.48xlarge',    # GH200 Grace Hopper variant
            
            # GB200 instances
            'p7.48xlarge',     # GB200
            
            # A10G instances
            'g5.xlarge', 'g5.2xlarge', 'g5.4xlarge', 'g5.8xlarge', 'g5.16xlarge', 'g5.12xlarge', 'g5.24xlarge', 'g5.48xlarge',
            'g6e.xlarge', 'g6e.2xlarge', 'g6e.4xlarge', 'g6e.8xlarge', 'g6e.16xlarge', 'g6e.12xlarge', 'g6e.24xlarge', 'g6e.48xlarge',
        ]
        
        for instance_type in latest_instance_types:
            if instance_type not in gpu_instance_types:
                # Try to see if instance is available in this region before adding it
                try:
                    # This will trigger an exception if the instance type doesn't exist in this region
                    ec2_client.describe_instance_type_offerings(
                        LocationType='region',
                        Filters=[
                            {
                                'Name': 'instance-type',
                                'Values': [instance_type]
                            }
                        ]
                    )
                    # If no exception is raised, add the instance type
                    gpu_instance_types.append(instance_type)
                    log_debug(f"Added latest GPU instance type {instance_type} to region {region}")
                except Exception as e:
                    log_debug(f"Instance type {instance_type} not available in region {region}: {e}")
                    continue
        
        # Process each GPU instance type
        for instance_type in gpu_instance_types:
            # Get GPU information for this instance type
            gpu_info = get_gpu_info(instance_type, ec2_client)
            
            # Check availability
            is_available = check_availability(ec2_client, instance_type)
            
            # Get pricing information
            spot_price = get_spot_price(ec2_client, instance_type)
            on_demand_price = get_on_demand_price(pricing_client, instance_type, region)
            
            # Create result entry
            result = {
                'instance_type': instance_type,
                'region': region,
                'available': is_available,
                'offered_in_region': True,
                'on_demand_price': on_demand_price,
                'spot_available': spot_price is not None,
                'spot_price': spot_price,
                'check_time': datetime.now().isoformat(),
                'gpu_type': gpu_info['gpu_type'],
                'gpu_memory': gpu_info['gpu_memory'],
                'gpu_count': gpu_info['gpu_count']
            }
            
            results.append(result)
            
            if DEBUG:
                price_str = f"${on_demand_price}/hr" if on_demand_price else "Unknown"
                spot_str = f"${spot_price}/hr" if spot_price else "None"
                log_debug(f"{instance_type} in {region}: Available={is_available}, ODPrice={price_str}, Spot={spot_str}")
        
    except Exception as e:
        log_error(f"Error checking region {region}: {e}")
    
    return results

def save_to_mysql(results):
    """Save the results to MySQL database with rounded spot prices"""
    try:
        # Connect to MySQL database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Prepare SQL statement
        sql = """
        INSERT INTO aws_gpu_instances (
            instance_type, region, available, offered_in_region, 
            on_demand_price, spot_available, spot_price, gpu_type, 
            gpu_memory, gpu_count, check_time
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Process each result
        records_inserted = 0
        for result in results:
            # Convert ISO format datetime string to MySQL datetime
            check_time = datetime.fromisoformat(result.get('check_time')).strftime('%Y-%m-%d %H:%M:%S')
            
            # Round prices to 2 decimal places if they exist
            on_demand_price = result.get('on_demand_price')
            if on_demand_price is not None:
                on_demand_price = round(on_demand_price * 100) / 100
                
            spot_price = result.get('spot_price')
            if spot_price is not None:
                spot_price = round(spot_price * 100) / 100
            
            # Prepare values tuple
            values = (
                result.get('instance_type'),
                result.get('region'),
                result.get('available', False),
                result.get('offered_in_region', False),
                on_demand_price,
                result.get('spot_available'),
                spot_price,
                result.get('gpu_type'),
                result.get('gpu_memory'),
                result.get('gpu_count'),
                check_time
            )
            
            try:
                # Execute query
                cursor.execute(sql, values)
                records_inserted += 1
            except mysql.connector.Error as e:
                log_error(f"Error inserting record for {result.get('instance_type')} in {result.get('region')}: {e}")
        
        # Commit transaction
        conn.commit()
        log_info(f"Successfully inserted {records_inserted} records into MySQL database.")
        
    except mysql.connector.Error as e:
        log_error(f"Error saving to MySQL: {e}")
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def main():
    """Main function to check GPU availability across regions and save to MySQL"""
    print("\n" + "="*80)
    print("AWS GPU CAPACITY CHECKER")
    print("="*80)
    
    # Verify that credentials are available
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        log_error("AWS credentials not found in environment variables.")
        log_error("Make sure you have created a .env file with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        return
    
    # Verify database setup
    if not verify_database_table():
        log_error("Database setup failed. Exiting.")
        return
    
    # Get all AWS regions
    regions = get_all_regions()
    print(f"Checking GPU availability across {len(regions)} AWS regions...")
    
    # Keep track of timing
    start_time = time.time()
    all_results = []
    
    # Use ThreadPoolExecutor to check regions in parallel
    # Limit max_workers to avoid overwhelming the API or the system
    with ThreadPoolExecutor(max_workers=min(len(regions), 10)) as executor:
        region_results = list(executor.map(check_gpu_availability_for_region, regions))
        
    # Flatten results from all regions
    for region_result in region_results:
        all_results.extend(region_result)
    
    # Generate a timestamp for the filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # # Write results to a JSON file
    # filename = f"aws_gpu_availability_{timestamp}.json"
    # with open(filename, 'w') as f:
    #     json.dump(all_results, f, indent=2)
    
    # Save results to MySQL database
    save_to_mysql(all_results)
    
    # Print summary
    available_instances = [r for r in all_results if r.get('available', False)]
    print(f"\nCompleted in {round(time.time() - start_time, 2)} seconds.")
    print(f"Found {len(available_instances)} available GPU instances out of {len(all_results)} total checks.")
    print(f"Results saved to MySQL database.")
    
    # Print a quick availability summary
    print("\nAvailability Summary:")
    summary = {}
    for result in all_results:
        if result.get('offered_in_region', False):
            key = f"{result['instance_type']} in {result['region']}"
            status = "AVAILABLE" if result.get('available', False) else "NOT AVAILABLE"
            on_demand = f"${result.get('on_demand_price'):.2f}/hr" if result.get('on_demand_price') else "Unknown"
            spot = f"${result.get('spot_price'):.2f}/hr" if result.get('spot_price') else "No spot"
            summary[key] = f"{status}, On-demand: {on_demand}, Spot: {spot}"
    
    for key, status in sorted(summary.items()):
        print(f"{key}: {status}")

if __name__ == "__main__":
    main()