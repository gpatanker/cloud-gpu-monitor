#!/usr/bin/env python3
import json
import time
import os
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import compute_v1
from google.cloud.billing import CloudCatalogClient
from google.api_core.exceptions import GoogleAPIError
import logging
import re
from functools import lru_cache
from collections import defaultdict

# Load environment variables from .env file
load_dotenv()

# Set to True to enable more detailed debug information
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('gcp_capacity_checker')

# MySQL database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'cloud_capacity')
}

# Priority regions to check first (major regions with most GPU availability)
PRIORITY_REGIONS = [
    'us-central1',    # Iowa
    'us-east1',       # South Carolina
    'us-east4',       # Northern Virginia
    'us-west1',       # Oregon
    'us-west2',       # Los Angeles
    'europe-west4',   # Netherlands
    'europe-west1',   # Belgium
    'asia-east1',     # Taiwan
    'asia-northeast1', # Tokyo
    'asia-southeast1'  # Singapore
]

# Default GCP regions with known GPU availability (full list)
GCP_REGIONS = PRIORITY_REGIONS + [
    # Additional regions
    'us-west4',       # Las Vegas
    'northamerica-northeast1',  # Montreal
    'southamerica-east1',       # Sao Paulo
    'europe-west2',     # London
    'europe-west3',     # Frankfurt
    'europe-north1',    # Finland
    'europe-southwest1', # Madrid
    'europe-west8',      # Milan
    'europe-west9',      # Paris
    'asia-east2',       # Hong Kong
    'asia-northeast2',  # Osaka
    'asia-northeast3',  # Seoul
    'asia-southeast2',  # Jakarta
    'asia-south1',      # Mumbai
    'asia-south2',      # Delhi
    'australia-southeast1', # Sydney
    'australia-southeast2', # Melbourne
    'me-west1',         # Tel Aviv
    'me-central1',      # Doha
    'africa-south1',    # Johannesburg
]

# GPU information mapping including all the latest GPUs
GPU_INFO = {
    # A2 - NVIDIA A100 40GB
    'a2-highgpu-1g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '40 GB', 'gpu_count': 1, 'vcpus': 12, 'memory_gb': 85},
    'a2-highgpu-2g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '40 GB', 'gpu_count': 2, 'vcpus': 24, 'memory_gb': 170},
    'a2-highgpu-4g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '40 GB', 'gpu_count': 4, 'vcpus': 48, 'memory_gb': 340},
    'a2-highgpu-8g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '40 GB', 'gpu_count': 8, 'vcpus': 96, 'memory_gb': 680},
    'a2-megagpu-16g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '40 GB', 'gpu_count': 16, 'vcpus': 112, 'memory_gb': 1360},
    
    # A2 with A100 80GB
    'a2-ultragpu-1g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '80 GB', 'gpu_count': 1, 'vcpus': 12, 'memory_gb': 170},
    'a2-ultragpu-2g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '80 GB', 'gpu_count': 2, 'vcpus': 24, 'memory_gb': 340},
    'a2-ultragpu-4g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '80 GB', 'gpu_count': 4, 'vcpus': 48, 'memory_gb': 680},
    'a2-ultragpu-8g': {'gpu_type': 'NVIDIA A100', 'gpu_memory': '80 GB', 'gpu_count': 8, 'vcpus': 96, 'memory_gb': 1360},
    
    # A3 - NVIDIA H100 80GB
    'a3-highgpu-8g': {'gpu_type': 'NVIDIA H100', 'gpu_memory': '80 GB', 'gpu_count': 8, 'vcpus': 208, 'memory_gb': 1872},
    'a3-megagpu-8g': {'gpu_type': 'NVIDIA H100', 'gpu_memory': '80 GB', 'gpu_count': 8, 'vcpus': 208, 'memory_gb': 1872},
    'a3-edgegpu-8g': {'gpu_type': 'NVIDIA H100', 'gpu_memory': '80 GB', 'gpu_count': 8, 'vcpus': 208, 'memory_gb': 1872},
    'a3-edgegpu-8g-nolssd': {'gpu_type': 'NVIDIA H100', 'gpu_memory': '80 GB', 'gpu_count': 8, 'vcpus': 208, 'memory_gb': 1872},
    'a3-highgpu-1g': {'gpu_type': 'NVIDIA H100', 'gpu_memory': '80 GB', 'gpu_count': 1, 'vcpus': 26, 'memory_gb': 234},
    'a3-highgpu-2g': {'gpu_type': 'NVIDIA H100', 'gpu_memory': '80 GB', 'gpu_count': 2, 'vcpus': 52, 'memory_gb': 468},
    'a3-highgpu-4g': {'gpu_type': 'NVIDIA H100', 'gpu_memory': '80 GB', 'gpu_count': 4, 'vcpus': 104, 'memory_gb': 936},
    
    # A4 - NVIDIA H200 and GH200 machine types
    'a4-highgpu-1g': {'gpu_type': 'NVIDIA H200', 'gpu_memory': '141 GB', 'gpu_count': 1, 'vcpus': 24, 'memory_gb': 256},
    'a4-highgpu-2g': {'gpu_type': 'NVIDIA H200', 'gpu_memory': '141 GB', 'gpu_count': 2, 'vcpus': 48, 'memory_gb': 512},
    'a4-highgpu-4g': {'gpu_type': 'NVIDIA H200', 'gpu_memory': '141 GB', 'gpu_count': 4, 'vcpus': 96, 'memory_gb': 1024},
    'a4-highgpu-8g': {'gpu_type': 'NVIDIA H200', 'gpu_memory': '141 GB', 'gpu_count': 8, 'vcpus': 192, 'memory_gb': 2048},
    'a4-megagpu-8g': {'gpu_type': 'NVIDIA H200', 'gpu_memory': '141 GB', 'gpu_count': 8, 'vcpus': 192, 'memory_gb': 2560},
    
    # A10 Machine Types (standalone)
    'a10-highgpu-1g': {'gpu_type': 'NVIDIA A10', 'gpu_memory': '24 GB', 'gpu_count': 1, 'vcpus': 12, 'memory_gb': 85},
    'a10-highgpu-2g': {'gpu_type': 'NVIDIA A10', 'gpu_memory': '24 GB', 'gpu_count': 2, 'vcpus': 24, 'memory_gb': 170},
    'a10-highgpu-4g': {'gpu_type': 'NVIDIA A10', 'gpu_memory': '24 GB', 'gpu_count': 4, 'vcpus': 48, 'memory_gb': 340},
    'a10-highgpu-8g': {'gpu_type': 'NVIDIA A10', 'gpu_memory': '24 GB', 'gpu_count': 8, 'vcpus': 96, 'memory_gb': 680},
    
    # GH200 Grace Hopper machine types
    'gh200-highgpu-1g': {'gpu_type': 'NVIDIA GH200', 'gpu_memory': '96 GB', 'gpu_count': 1, 'vcpus': 24, 'memory_gb': 256},
    'gh200-megagpu-4g': {'gpu_type': 'NVIDIA GH200', 'gpu_memory': '96 GB', 'gpu_count': 4, 'vcpus': 96, 'memory_gb': 1024},
    'gh200-megagpu-8g': {'gpu_type': 'NVIDIA GH200', 'gpu_memory': '96 GB', 'gpu_count': 8, 'vcpus': 192, 'memory_gb': 2048},
    
    # GB200 Blackwell machine types
    'gb200-highgpu-1g': {'gpu_type': 'NVIDIA GB200', 'gpu_memory': '132 GB', 'gpu_count': 1, 'vcpus': 32, 'memory_gb': 384},
    'gb200-highgpu-2g': {'gpu_type': 'NVIDIA GB200', 'gpu_memory': '132 GB', 'gpu_count': 2, 'vcpus': 64, 'memory_gb': 768},
    'gb200-megagpu-4g': {'gpu_type': 'NVIDIA GB200', 'gpu_memory': '132 GB', 'gpu_count': 4, 'vcpus': 128, 'memory_gb': 1536},
    'gb200-megagpu-8g': {'gpu_type': 'NVIDIA GB200', 'gpu_memory': '132 GB', 'gpu_count': 8, 'vcpus': 256, 'memory_gb': 3072},
    
    # G2 - NVIDIA L4
    'g2-standard-4': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 1, 'vcpus': 4, 'memory_gb': 16},
    'g2-standard-8': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 1, 'vcpus': 8, 'memory_gb': 32},
    'g2-standard-12': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 1, 'vcpus': 12, 'memory_gb': 48},
    'g2-standard-16': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 1, 'vcpus': 16, 'memory_gb': 64},
    'g2-standard-24': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 2, 'vcpus': 24, 'memory_gb': 96},
    'g2-standard-32': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 2, 'vcpus': 32, 'memory_gb': 128},
    'g2-standard-48': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 4, 'vcpus': 48, 'memory_gb': 192},
    'g2-standard-96': {'gpu_type': 'NVIDIA L4', 'gpu_memory': '24 GB', 'gpu_count': 8, 'vcpus': 96, 'memory_gb': 384},
    
    # N1 with T4
    'n1-standard-1-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 1, 'memory_gb': 3.75},
    'n1-standard-2-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 2, 'memory_gb': 7.5},
    'n1-standard-4-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 4, 'memory_gb': 15},
    'n1-standard-8-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 8, 'memory_gb': 30},
    'n1-standard-16-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 16, 'memory_gb': 60},
    'n1-standard-32-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 32, 'memory_gb': 120},
    'n1-standard-48-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 48, 'memory_gb': 180},
    'n1-standard-64-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 64, 'memory_gb': 240},
    'n1-standard-96-t4-small': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 0.25, 'vcpus': 96, 'memory_gb': 360},
    
    # N1 with T4 full
    'n1-standard-2-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 2, 'memory_gb': 7.5},
    'n1-standard-4-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 4, 'memory_gb': 15},
    'n1-standard-8-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 8, 'memory_gb': 30},
    'n1-standard-16-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 16, 'memory_gb': 60},
    'n1-standard-32-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 32, 'memory_gb': 120},
    'n1-standard-48-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 48, 'memory_gb': 180},
    'n1-standard-64-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 64, 'memory_gb': 240},
    'n1-standard-96-t4': {'gpu_type': 'NVIDIA T4', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 96, 'memory_gb': 360},
    
    # N1 with V100
    'n1-standard-8-v100': {'gpu_type': 'NVIDIA Tesla V100', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 8, 'memory_gb': 30},
    'n1-standard-16-v100': {'gpu_type': 'NVIDIA Tesla V100', 'gpu_memory': '16 GB', 'gpu_count': 2, 'vcpus': 16, 'memory_gb': 60},
    'n1-standard-32-v100': {'gpu_type': 'NVIDIA Tesla V100', 'gpu_memory': '16 GB', 'gpu_count': 4, 'vcpus': 32, 'memory_gb': 120},
    'n1-standard-64-v100': {'gpu_type': 'NVIDIA Tesla V100', 'gpu_memory': '16 GB', 'gpu_count': 8, 'vcpus': 64, 'memory_gb': 240},
    'n1-standard-96-v100': {'gpu_type': 'NVIDIA Tesla V100', 'gpu_memory': '16 GB', 'gpu_count': 8, 'vcpus': 96, 'memory_gb': 360},
    
    # N1 with P100
    'n1-standard-8-p100': {'gpu_type': 'NVIDIA Tesla P100', 'gpu_memory': '16 GB', 'gpu_count': 1, 'vcpus': 8, 'memory_gb': 30},
    'n1-standard-16-p100': {'gpu_type': 'NVIDIA Tesla P100', 'gpu_memory': '16 GB', 'gpu_count': 2, 'vcpus': 16, 'memory_gb': 60},
    'n1-standard-32-p100': {'gpu_type': 'NVIDIA Tesla P100', 'gpu_memory': '16 GB', 'gpu_count': 4, 'vcpus': 32, 'memory_gb': 120},
    'n1-standard-64-p100': {'gpu_type': 'NVIDIA Tesla P100', 'gpu_memory': '16 GB', 'gpu_count': 8, 'vcpus': 64, 'memory_gb': 240},
    'n1-standard-96-p100': {'gpu_type': 'NVIDIA Tesla P100', 'gpu_memory': '16 GB', 'gpu_count': 8, 'vcpus': 96, 'memory_gb': 360},
}

# Hardcoded pricing for popular GPU machine types (in case API fails)
DEFAULT_PRICING = {
    # A100 40GB machine types
    'a2-highgpu-1g': {'on_demand': 3.67, 'spot': 1.10},
    'a2-highgpu-2g': {'on_demand': 7.35, 'spot': 2.20},
    'a2-highgpu-4g': {'on_demand': 14.69, 'spot': 4.41},
    'a2-highgpu-8g': {'on_demand': 29.38, 'spot': 8.81},
    'a2-megagpu-16g': {'on_demand': 58.77, 'spot': 17.63},
    
    # A100 80GB machine types
    'a2-ultragpu-1g': {'on_demand': 5.50, 'spot': 1.65},
    'a2-ultragpu-2g': {'on_demand': 11.00, 'spot': 3.30},
    'a2-ultragpu-4g': {'on_demand': 22.01, 'spot': 6.60},
    'a2-ultragpu-8g': {'on_demand': 44.02, 'spot': 13.21},
    
    # H100 machine types
    'a3-highgpu-8g': {'on_demand': 68.03, 'spot': 22.45},
    'a3-megagpu-8g': {'on_demand': 81.64, 'spot': 27.85},
    'a3-highgpu-1g': {'on_demand': 9.40, 'spot': 3.10},
    'a3-highgpu-2g': {'on_demand': 18.80, 'spot': 6.20},
    'a3-highgpu-4g': {'on_demand': 37.60, 'spot': 12.40},
    
    # H200 machine types
    'a4-highgpu-1g': {'on_demand': 12.80, 'spot': 4.22},
    'a4-highgpu-2g': {'on_demand': 25.60, 'spot': 8.45},
    'a4-highgpu-4g': {'on_demand': 51.20, 'spot': 16.90},
    'a4-highgpu-8g': {'on_demand': 102.40, 'spot': 33.80},
    'a4-megagpu-8g': {'on_demand': 118.76, 'spot': 39.19},
    
    # A10 machine types
    'a10-highgpu-1g': {'on_demand': 2.40, 'spot': 0.80},
    'a10-highgpu-2g': {'on_demand': 4.80, 'spot': 1.60},
    'a10-highgpu-4g': {'on_demand': 9.60, 'spot': 3.20},
    'a10-highgpu-8g': {'on_demand': 19.20, 'spot': 6.40},
    
    # GH200 machine types
    'gh200-highgpu-1g': {'on_demand': 14.50, 'spot': 4.80},
    'gh200-megagpu-4g': {'on_demand': 58.00, 'spot': 19.20},
    'gh200-megagpu-8g': {'on_demand': 116.00, 'spot': 38.40},
    
    # GB200 machine types
    'gb200-highgpu-1g': {'on_demand': 18.90, 'spot': 6.24},
    'gb200-highgpu-2g': {'on_demand': 37.80, 'spot': 12.47},
    'gb200-megagpu-4g': {'on_demand': 75.60, 'spot': 24.95},
    'gb200-megagpu-8g': {'on_demand': 151.20, 'spot': 49.90},
    
    # L4 machine types
    'g2-standard-4': {'on_demand': 1.20, 'spot': 0.38},
    'g2-standard-8': {'on_demand': 1.53, 'spot': 0.48},
    'g2-standard-16': {'on_demand': 2.19, 'spot': 0.68},
    'g2-standard-32': {'on_demand': 3.55, 'spot': 1.10},
    'g2-standard-48': {'on_demand': 6.99, 'spot': 2.16},
    'g2-standard-96': {'on_demand': 13.98, 'spot': 4.32},
    
    # T4 machine types
    'n1-standard-4-t4': {'on_demand': 1.05, 'spot': 0.31},
    'n1-standard-8-t4': {'on_demand': 1.34, 'spot': 0.42},
    'n1-standard-16-t4': {'on_demand': 2.14, 'spot': 0.67},
    'n1-standard-32-t4': {'on_demand': 3.73, 'spot': 1.15},
    
    # V100 machine types
    'n1-standard-8-v100': {'on_demand': 2.48, 'spot': 0.74},
    'n1-standard-16-v100': {'on_demand': 5.32, 'spot': 1.59},
    'n1-standard-32-v100': {'on_demand': 10.64, 'spot': 3.19},
    'n1-standard-64-v100': {'on_demand': 21.28, 'spot': 6.38},
}

# Cache to avoid repeated API calls
machine_types_cache = {}
accelerator_types_cache = {}
region_zones_cache = {}
pricing_cache = {}
sku_list_cache = None
sku_list_cache_time = 0

# Timeout settings (in seconds)
REGION_CHECK_TIMEOUT = 120  # 2 minutes per region
API_TIMEOUT = 30  # 30 seconds for individual API calls

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

def log_warning(message):
    """Helper function for warning logging"""
    logger.warning(message)

@lru_cache(maxsize=1000)
def standardize_gpu_type_naming(gpu_type):
    """
    Standardize GPU type naming dynamically, keeping memory size separate.
    Uses LRU caching to improve performance.
    """
    if not gpu_type or gpu_type.lower() in ["unknown", "n/a", "none"]:
        return 'Unknown GPU'
        
    # Handle normalized case and remove any extra spaces
    gpu_type = gpu_type.strip()
    
    # Remove memory specifications if they're part of the GPU type
    gpu_type = re.sub(r'\s+\d+GB', '', gpu_type, flags=re.IGNORECASE)
    gpu_type = re.sub(r'\s+\d+\s*GB', '', gpu_type, flags=re.IGNORECASE)
    gpu_type = re.sub(r'-\d+gb', '', gpu_type, flags=re.IGNORECASE)
    
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
    
    # Known GPU families mapping
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
            # Format model name - uppercase letters with numbers, but don't include memory size
            model = ''.join(c.upper() if i > 0 and c.isdigit() else c 
                          for i, c in enumerate(gpu_type))
            return f"{vendor} {model}"
    
    # Default to NVIDIA for unrecognized GPUs
    return f"NVIDIA {gpu_type}"

def verify_database_table():
    """Verify that the gcp_gpu_instances table exists, creating it if needed"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if the table exists
        cursor.execute("SHOW TABLES LIKE 'gcp_gpu_instances'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            log_info("Creating gcp_gpu_instances table...")
            create_table_sql = """
            CREATE TABLE `gcp_gpu_instances` (
              `id` int NOT NULL AUTO_INCREMENT,
              `machine_type` varchar(255) NOT NULL,
              `region` varchar(255) NOT NULL,
              `available` tinyint(1) DEFAULT '0',
              `offered_in_region` tinyint(1) DEFAULT '0',
              `on_demand_price` decimal(10,4) DEFAULT NULL,
              `spot_available` tinyint(1) DEFAULT '0',
              `spot_price` decimal(10,4) DEFAULT NULL,
              `gpu_type` varchar(255) DEFAULT NULL,
              `gpu_memory` varchar(255) DEFAULT NULL,
              `gpu_count` float DEFAULT '0',
              `vcpus` int DEFAULT '0',
              `memory_gb` int DEFAULT '0',
              `check_time` datetime DEFAULT NULL,
              `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              KEY `region_machine_type_idx` (`region`,`machine_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_table_sql)
            conn.commit()
            log_info("Table created successfully")
        else:
            # Check if on_demand_price column exists, add it if not
            cursor.execute("SHOW COLUMNS FROM gcp_gpu_instances LIKE 'on_demand_price'")
            column_exists = cursor.fetchone() is not None
            
            if not column_exists:
                log_info("Adding on_demand_price column to gcp_gpu_instances table...")
                cursor.execute("ALTER TABLE gcp_gpu_instances ADD COLUMN on_demand_price decimal(10,4) DEFAULT NULL AFTER offered_in_region")
                conn.commit()
                log_info("Column added successfully")
                
            # Check if vcpus and memory_gb columns exist, add them if not
            cursor.execute("SHOW COLUMNS FROM gcp_gpu_instances LIKE 'vcpus'")
            column_exists = cursor.fetchone() is not None
            
            if not column_exists:
                log_info("Adding vcpus and memory_gb columns to gcp_gpu_instances table...")
                cursor.execute("ALTER TABLE gcp_gpu_instances ADD COLUMN vcpus int DEFAULT '0' AFTER gpu_count")
                cursor.execute("ALTER TABLE gcp_gpu_instances ADD COLUMN memory_gb int DEFAULT '0' AFTER vcpus")
                conn.commit()
                log_info("Columns added successfully")
        
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as e:
        log_error(f"Database error: {e}")
        return False

def get_all_regions(compute_client, project_id):
    """Get a list of all available GCP regions with priority regions first"""
    try:
        regions_client = compute_v1.RegionsClient()
        all_regions = []
        
        request = regions_client.list(project=project_id)
        for region in request:
            all_regions.append(region.name)
        
        # Sort with priority regions first
        sorted_regions = []
        
        # Add priority regions that exist in the account
        for priority_region in PRIORITY_REGIONS:
            if priority_region in all_regions:
                sorted_regions.append(priority_region)
                all_regions.remove(priority_region)
        
        # Add remaining regions
        sorted_regions.extend(sorted(all_regions))
        
        return sorted_regions
    except Exception as e:
        log_error(f"Error getting regions: {e}")
        # Fallback to predefined regions with priority first
        return PRIORITY_REGIONS + [r for r in GCP_REGIONS if r not in PRIORITY_REGIONS]

def get_zones_for_region(compute_client, project_id, region):
    """Get all zones for a specific region with caching"""
    if region in region_zones_cache:
        return region_zones_cache[region]
    
    try:
        zones_client = compute_v1.ZonesClient()
        zones = []
        
        request = zones_client.list(project=project_id)
        for zone in request:
            if zone.name.startswith(f"{region}-"):
                zones.append(zone.name)
        
        if not zones:
            log_debug(f"No zones found for region {region}")
            return []
        
        region_zones_cache[region] = zones
        return zones
    except Exception as e:
        log_error(f"Error getting zones for region {region}: {e}")
        # Fallback to standard zone naming
        potential_zones = [f"{region}-a", f"{region}-b", f"{region}-c"]
        return [z for z in potential_zones if z in GCP_REGIONS]

def get_accelerator_types(compute_client, project_id, zone):
    """Get all available accelerator types in a zone with caching"""
    if zone in accelerator_types_cache:
        return accelerator_types_cache[zone]
    
    try:
        accelerator_client = compute_v1.AcceleratorTypesClient()
        request = accelerator_client.list(project=project_id, zone=zone)
        
        accelerator_types = []
        for acc_type in request:
            # Only include GPU accelerators (filter out TPUs, etc.)
            if any(gpu_keyword in acc_type.name.lower() for gpu_keyword in 
                  ["nvidia", "gpu", "tesla", "v100", "a100", "h100", "h200", "t4", "p100", "l4", "a10", "gh200", "gb200"]):
                info = {
                    'name': acc_type.name,
                    'description': acc_type.description,
                    'maximum_cards_per_instance': acc_type.maximum_cards_per_instance
                }
                accelerator_types.append(info)
        
        accelerator_types_cache[zone] = accelerator_types
        return accelerator_types
    except Exception as e:
        log_debug(f"Error getting accelerator types for zone {zone}: {e}")
        accelerator_types_cache[zone] = []
        return []

def get_machine_type_details(compute_client, project_id, zone, machine_type_name):
    """Get detailed information about a specific machine type with caching"""
    cache_key = f"{zone}:{machine_type_name}"
    if cache_key in machine_types_cache:
        return machine_types_cache[cache_key]
    
    try:
        # Get the machine type details with timeout
        machine_type = compute_client.get(
            project=project_id, 
            zone=zone, 
            machine_type=machine_type_name,
            timeout=API_TIMEOUT
        )
        machine_types_cache[cache_key] = machine_type
        return machine_type
    except Exception as e:
        log_debug(f"Error getting machine type details for {machine_type_name} in {zone}: {e}")
        machine_types_cache[cache_key] = None
        return None

def get_gpu_machine_types(compute_client, project_id, zone):
    """Get machine types with GPU support for a specific zone with caching"""
    if zone in machine_types_cache:
        return machine_types_cache[zone]
    
    try:
        # Get all machine types available in the zone with timeout
        request = compute_client.list(project=project_id, zone=zone, timeout=API_TIMEOUT)
        
        machine_types = []
        gpu_machine_prefixes = ["a2-", "a3-", "a4-", "a10-", "g2-", "gh200-", "gb200-"]
        n1_gpu_suffixes = ["-t4", "-v100", "-p100", "-p4"]
        
        for machine_type in request:
            # Check if it's a predefined GPU machine type
            if any(machine_type.name.startswith(prefix) for prefix in gpu_machine_prefixes):
                machine_types.append(machine_type.name)
            # Check for N1 machines with GPU suffixes
            elif machine_type.name.startswith("n1-") and any(suffix in machine_type.name for suffix in n1_gpu_suffixes):
                machine_types.append(machine_type.name)
            # Check for any machine type that supports GPUs based on accelerators
            elif hasattr(machine_type, 'accelerators') and machine_type.accelerators:
                machine_types.append(machine_type.name)
        
        # Also add known GPU machine types from our GPU_INFO dictionary
        # This helps in case the API doesn't return all machine types
        for custom_gpu_machine in GPU_INFO.keys():
            if custom_gpu_machine not in machine_types:
                try:
                    # Try to get details for this machine type to verify it exists in this zone
                    details = get_machine_type_details(compute_client, project_id, zone, custom_gpu_machine)
                    if details:
                        machine_types.append(custom_gpu_machine)
                except:
                    # If we can't get details, assume it's not available in this zone
                    pass
        
        machine_types_cache[zone] = machine_types
        return machine_types
    except Exception as e:
        log_debug(f"Error getting machine types for zone {zone}: {e}")
        machine_types_cache[zone] = []
        return []

def initialize_billing_client():
    """Initialize and return a Cloud Billing API client"""
    try:
        client = CloudCatalogClient()
        return client
    except Exception as e:
        log_error(f"Error initializing Cloud Billing client: {e}")
        return None

def get_cached_sku_list(billing_client):
    """Get and cache the SKU list with a 1-hour freshness"""
    global sku_list_cache, sku_list_cache_time
    
    current_time = time.time()
    if sku_list_cache and (current_time - sku_list_cache_time) < 3600:  # 1 hour cache
        return sku_list_cache
    
    try:
        # Cloud Catalog API name for Compute Engine
        parent = "services/6F81-5844-456A"  # Compute Engine service ID
        
        # List all SKUs with timeout
        sku_list = list(billing_client.list_skus(parent=parent, timeout=API_TIMEOUT))
        
        sku_list_cache = sku_list
        sku_list_cache_time = current_time
        return sku_list
    except Exception as e:
        log_error(f"Error getting SKU list: {e}")
        return None

def get_dynamic_pricing_info(billing_client, machine_type, gpu_type, region):
    """
    Get dynamic pricing information for a machine type with optimized caching.
    """
    cache_key = f"{machine_type}:{gpu_type}:{region}"
    if cache_key in pricing_cache:
        return pricing_cache[cache_key]
    
    if not billing_client:
        log_debug(f"No billing client available for pricing of {machine_type}")
        result = fallback_to_default_pricing(machine_type)
        pricing_cache[cache_key] = result
        return result
    
    try:
        # Get cached SKU list
        sku_list = get_cached_sku_list(billing_client)
        if not sku_list:
            log_debug("Using fallback pricing due to SKU list retrieval failure")
            result = fallback_to_default_pricing(machine_type)
            pricing_cache[cache_key] = result
            return result
        
        # We'll collect all relevant pricing information
        on_demand_price = None
        spot_price = None
        
        # Define search terms to match this machine type
        machine_search_terms = []
        
        # Extract family and size from machine type
        parts = machine_type.split('-')
        if len(parts) >= 2:
            family = parts[0]
            machine_search_terms.append(family)
            
            # For specific machine types, add more search terms
            if family in ['a2', 'a3', 'a4', 'a10', 'g2', 'gh200', 'gb200']:
                # These are predefined GPU machine types
                machine_search_terms.append(machine_type)
                
                # Check for highgpu or ultragpu variants
                if len(parts) >= 3:
                    variant = parts[1]
                    if variant in ['highgpu', 'ultragpu', 'megagpu', 'edgegpu']:
                        machine_search_terms.append(f"{family}-{variant}")
            
            # For N1 machines with GPU
            if family == 'n1' and 'gpu' in machine_type:
                machine_search_terms.append('gpu')
                # Add GPU type as search term
                if 't4' in machine_type:
                    machine_search_terms.append('t4')
                elif 'v100' in machine_type:
                    machine_search_terms.append('v100')
                elif 'p100' in machine_type:
                    machine_search_terms.append('p100')
                    
        # Normalize region for matching
        region_code = region.replace('-', '').lower()
        
        log_debug(f"Searching for pricing with terms: {machine_search_terms} in region {region}")
        
        # Collect matching SKUs
        matching_skus_ondemand = []
        matching_skus_spot = []
        
        for sku in sku_list:
            description = sku.description.lower()
            
            # Check if this SKU matches our machine type
            matches_machine = False
            for term in machine_search_terms:
                if term.lower() in description:
                    matches_machine = True
                    break
            
            if not matches_machine:
                continue
                
            # Check for GPU relevance
            has_gpu_relevance = False
            if gpu_type and gpu_type.lower() != 'unknown':
                gpu_type_lower = gpu_type.lower()
                if (gpu_type_lower in description or 
                    'gpu' in description or 
                    'accelerator' in description):
                    has_gpu_relevance = True
            else:
                # If we don't know the GPU type, be more inclusive
                if ('gpu' in description or 'accelerator' in description):
                    has_gpu_relevance = True
            
            if not has_gpu_relevance:
                continue
                
            # Check region match
            region_match = False
            if hasattr(sku, 'geo_taxonomy') and hasattr(sku.geo_taxonomy, 'regions'):
                for geo_region in sku.geo_taxonomy.regions:
                    if region_code in geo_region.lower().replace('-', ''):
                        region_match = True
                        break
            
            if not region_match:
                continue
            
            # Determine if this is on-demand or spot pricing
            is_spot = 'spot' in description.lower()
            
            # Get the price from this SKU
            price = None
            for pricing_info in sku.pricing_info:
                if pricing_info.pricing_expression.tiered_rates:
                    rate = pricing_info.pricing_expression.tiered_rates[0]
                    if hasattr(rate.unit_price, 'units') and hasattr(rate.unit_price, 'nanos'):
                        price = float(rate.unit_price.units) + (rate.unit_price.nanos / 1e9)
                        break
            
            if price is None:
                continue
                
            # Store this SKU with its price based on type
            if is_spot:
                matching_skus_spot.append((sku, price))
            else:
                matching_skus_ondemand.append((sku, price))
                
        # Find the best matching SKU for on-demand pricing
        if matching_skus_ondemand:
            # Sort by how likely this SKU is to be the correct one
            matching_skus_ondemand.sort(key=lambda x: match_score(x[0].description, machine_type, gpu_type), reverse=True)
            on_demand_price = matching_skus_ondemand[0][1]
            log_debug(f"On-demand price found for {machine_type}: ${on_demand_price}/hr")
            
        # Find the best matching SKU for spot pricing
        if matching_skus_spot:
            matching_skus_spot.sort(key=lambda x: match_score(x[0].description, machine_type, gpu_type), reverse=True)
            spot_price = matching_skus_spot[0][1]
            log_debug(f"Spot price found for {machine_type}: ${spot_price}/hr")
        
        # If we found pricing, cache and return it
        result = {
            'on_demand': on_demand_price,
            'spot': spot_price
        }
        pricing_cache[cache_key] = result
        
        # If we couldn't find pricing, fall back to default pricing
        if on_demand_price is None and spot_price is None:
            log_debug(f"No pricing found for {machine_type}, falling back to defaults")
            result = fallback_to_default_pricing(machine_type)
            pricing_cache[cache_key] = result
        
        return result
    
    except Exception as e:
        log_error(f"Error getting dynamic pricing for {machine_type}: {e}")
        result = fallback_to_default_pricing(machine_type)
        pricing_cache[cache_key] = result
        return result
    
def match_score(description, machine_type, gpu_type):
    """
    Calculate a score for how well a SKU description matches our machine and GPU type.
    Higher score = better match.
    """
    score = 0
    description = description.lower()
    
    # Check for exact machine type match
    if machine_type.lower() in description:
        score += 10
    
    # Check for machine family match
    family = machine_type.split('-')[0].lower()
    if family in description:
        score += 5
    
    # Check for GPU type match
    if gpu_type and gpu_type.lower() != 'unknown' and gpu_type.lower() in description:
        score += 8
    
    # Check for GPU count in name and description
    if 'gpu' in machine_type:
        for part in machine_type.split('-'):
            if part.endswith('g') and part[:-1].isdigit():
                gpu_count = part[:-1]
                if gpu_count in description:
                    score += 7
    
    # Prefer more specific descriptions
    specificity = len(description.split())
    score += min(specificity / 5, 3)  # Cap at 3 points
    
    return score

def fallback_to_default_pricing(machine_type):
    """
    Fallback to default hardcoded pricing when dynamic pricing retrieval fails.
    """
    # First check if we have it in our default pricing
    if machine_type in DEFAULT_PRICING:
        pricing = DEFAULT_PRICING[machine_type]
        return {
            'on_demand': pricing['on_demand'],
            'spot': pricing['spot']
        }
    
    # If we don't have it in default pricing, check if we have a similar machine type
    # This is useful for machine types with similar patterns
    for default_type, pricing in DEFAULT_PRICING.items():
        # Check if the machine type follows a similar pattern
        if (machine_type.startswith(default_type.split('-')[0]) and 
            '-'.join(machine_type.split('-')[1:]) == '-'.join(default_type.split('-')[1:])):
            return {
                'on_demand': pricing['on_demand'],
                'spot': pricing['spot']
            }
    
    # If we can't find any pricing information, return None
    return {
        'on_demand': None,
        'spot': None
    }

def get_machine_vcpus_memory(compute_client, project_id, zone, machine_type_name):
    """Get vCPUs and memory information for a machine type with caching"""
    cache_key = f"{zone}:{machine_type_name}:specs"
    if cache_key in machine_types_cache:
        return machine_types_cache[cache_key]
    
    try:
        machine_type = compute_client.get(
            project=project_id, 
            zone=zone, 
            machine_type=machine_type_name,
            timeout=API_TIMEOUT
        )
        result = {
            'vcpus': machine_type.guest_cpus,
            'memory_gb': machine_type.memory_mb // 1024  # Convert MB to GB
        }
        machine_types_cache[cache_key] = result
        return result
    except Exception as e:
        log_debug(f"Error getting machine details for {machine_type_name} in {zone}: {e}")
        
        # Check if we have this information in our GPU_INFO
        if machine_type_name in GPU_INFO:
            gpu_info = GPU_INFO[machine_type_name]
            result = {
                'vcpus': gpu_info.get('vcpus', 0),
                'memory_gb': gpu_info.get('memory_gb', 0)
            }
            machine_types_cache[cache_key] = result
            return result
        
        # Try to extract CPU info from the name for standard machine types
        if machine_type_name.startswith('n1-standard-') or machine_type_name.startswith('n2-standard-'):
            try:
                # Format is typically n1-standard-{cpu_count}
                parts = machine_type_name.split('-')
                if len(parts) >= 3:
                    cpu_part = parts[2].split('-')[0]  # Handle cases like n1-standard-8-v100
                    vcpus = int(cpu_part)
                    memory_gb = vcpus * 3.75  # Standard ratio for N1/N2
                    result = {
                        'vcpus': vcpus,
                        'memory_gb': int(memory_gb)
                    }
                    machine_types_cache[cache_key] = result
                    return result
            except:
                pass
                
        # For GPU-specific machine types, use the following estimates
        if machine_type_name.startswith('a2-highgpu-'):
            try:
                gpu_count = int(machine_type_name.split('-')[-1].replace('g', ''))
                result = {
                    'vcpus': 12 * gpu_count,
                    'memory_gb': 85 * gpu_count
                }
                machine_types_cache[cache_key] = result
                return result
            except:
                pass
        elif machine_type_name.startswith('a2-megagpu-'):
            try:
                gpu_count = int(machine_type_name.split('-')[-1].replace('g', ''))
                result = {
                    'vcpus': 7 * gpu_count,
                    'memory_gb': 40 * gpu_count
                }
                machine_types_cache[cache_key] = result
                return result
            except:
                pass
        elif machine_type_name.startswith('a3-highgpu-'):
            try:
                gpu_count = int(machine_type_name.split('-')[-1].replace('g', ''))
                result = {
                    'vcpus': 12 * gpu_count,
                    'memory_gb': 93 * gpu_count
                }
                machine_types_cache[cache_key] = result
                return result
            except:
                pass
        elif machine_type_name.startswith('a4-highgpu-') or machine_type_name.startswith('a4-megagpu-'):
            try:
                gpu_count = int(machine_type_name.split('-')[-1].replace('g', ''))
                result = {
                    'vcpus': 24 * gpu_count,
                    'memory_gb': 256 * gpu_count
                }
                machine_types_cache[cache_key] = result
                return result
            except:
                pass
        elif machine_type_name.startswith('a10-highgpu-'):
            try:
                gpu_count = int(machine_type_name.split('-')[-1].replace('g', ''))
                result = {
                    'vcpus': 12 * gpu_count,
                    'memory_gb': 85 * gpu_count
                }
                machine_types_cache[cache_key] = result
                return result
            except:
                pass
        elif machine_type_name.startswith('gh200-') or machine_type_name.startswith('gb200-'):
            try:
                gpu_count = int(machine_type_name.split('-')[-1].replace('g', ''))
                result = {
                    'vcpus': 24 * gpu_count,
                    'memory_gb': 256 * gpu_count
                }
                machine_types_cache[cache_key] = result
                return result
            except:
                pass
        elif machine_type_name.startswith('g2-standard-'):
            try:
                vcpus = int(machine_type_name.split('-')[-1])
                result = {
                    'vcpus': vcpus,
                    'memory_gb': vcpus * 4
                }
                machine_types_cache[cache_key] = result
                return result
            except:
                pass
                
        # Default values if we can't determine
        result = {
            'vcpus': 0,
            'memory_gb': 0
        }
        machine_types_cache[cache_key] = result
        return result

def check_gpu_availability_for_region(region_info):
    """Check GPU availability for a specific region with optimizations"""
    region, project_id, compute_client, billing_client = region_info
    results = []
    
    try:
        log_info(f"Checking GPU availability in region {region}")
        
        # Get zones for this region
        zones = get_zones_for_region(compute_client, project_id, region)
        if not zones:
            log_debug(f"No zones found for region {region}")
            return []
        
        # Use the first zone to check available GPU machine types
        primary_zone = zones[0]
        log_debug(f"Using primary zone {primary_zone} for {region}")
        
        # Get all machine types in this zone that support GPUs
        machine_types = get_gpu_machine_types(compute_client, project_id, primary_zone)
        log_debug(f"Found {len(machine_types)} potential GPU machine types in {primary_zone}")
        
        # Get all accelerator (GPU) types for this zone
        accelerator_types = get_accelerator_types(compute_client, project_id, primary_zone)
        
        # Process each machine type with optimizations
        for machine_type_name in machine_types:
            # Skip non-GPU machine types unless in debug mode
            if not DEBUG:
                # These are known non-GPU prefixes
                non_gpu_prefixes = ['e2-', 'n2d-', 'c2-', 'm1-']
                if any(machine_type_name.startswith(p) for p in non_gpu_prefixes):
                    continue
                    
                # These are known CPU-only machine types
                cpu_only = ['custom-', 'f1-', 'g1-']
                if any(mt in machine_type_name for mt in cpu_only):
                    continue
            
            # Initialize variables
            gpu_type = "Unknown"
            gpu_count = 0
            gpu_memory = "Unknown"
            vcpus = 0
            memory_gb = 0
            
            # Try to get GPU information from our mapping first (fastest path)
            if machine_type_name in GPU_INFO:
                gpu_info = GPU_INFO[machine_type_name]
                gpu_type = gpu_info['gpu_type']
                gpu_count = gpu_info['gpu_count']
                gpu_memory = gpu_info['gpu_memory']
                vcpus = gpu_info.get('vcpus', 0)
                memory_gb = gpu_info.get('memory_gb', 0)
            else:
                # Try to determine GPU type from the machine details
                machine_type_details = get_machine_type_details(compute_client, project_id, primary_zone, machine_type_name)
                
                if machine_type_details and hasattr(machine_type_details, 'accelerators') and machine_type_details.accelerators:
                    for acc in machine_type_details.accelerators:
                        if acc.guest_accelerator_count > 0:
                            # Find the matching accelerator type
                            for acc_type in accelerator_types:
                                if acc.guest_accelerator_type == acc_type['name']:
                                    gpu_type = standardize_gpu_type_naming(acc_type['name'])
                                    gpu_count = acc.guest_accelerator_count
                                    # Try to extract memory from description
                                    for mem_size in ["16 GB", "32 GB", "40 GB", "80 GB", "24 GB", "141 GB", "96 GB", "132 GB"]:
                                        if mem_size in acc_type['description']:
                                            gpu_memory = mem_size
                                            break
                                    break
                
                # If still unknown, try to infer from machine type name
                if gpu_type == "Unknown" or gpu_count == 0:
                    # Check for new machine types by prefix
                    prefix_to_gpu_info = {
                        "a2-highgpu": {"type": "NVIDIA A100", "memory": "40 GB"},
                        "a2-ultragpu": {"type": "NVIDIA A100", "memory": "80 GB"},
                        "a2-megagpu": {"type": "NVIDIA A100", "memory": "40 GB"},
                        "a3-": {"type": "NVIDIA H100", "memory": "80 GB"},
                        "a4-": {"type": "NVIDIA H200", "memory": "141 GB"},
                        "a10-": {"type": "NVIDIA A10", "memory": "24 GB"},
                        "gh200-": {"type": "NVIDIA GH200", "memory": "96 GB"},
                        "gb200-": {"type": "NVIDIA GB200", "memory": "132 GB"},
                        "g2-": {"type": "NVIDIA L4", "memory": "24 GB"}
                    }
                    
                    for prefix, info in prefix_to_gpu_info.items():
                        if machine_type_name.startswith(prefix):
                            gpu_type = info["type"]
                            gpu_memory = info["memory"]
                            # Extract GPU count from name suffix if available
                            for part in machine_type_name.split("-"):
                                if part.endswith("g") and part[:-1].isdigit():
                                    gpu_count = int(part[:-1])
                                    break
                            
                            # Special case for G2 instances
                            if prefix == "g2-":
                                if "standard-24" in machine_type_name:
                                    gpu_count = 2
                                elif "standard-48" in machine_type_name:
                                    gpu_count = 4
                                elif "standard-96" in machine_type_name:
                                    gpu_count = 8
                                else:
                                    gpu_count = 1
                            break
                    
                    # Check for specific GPU types in N1 instances
                    if "-t4" in machine_type_name:
                        gpu_type = "NVIDIA T4"
                        gpu_memory = "16 GB"
                        gpu_count = 1
                        if "t4-small" in machine_type_name:
                            gpu_count = 0.25  # Partial GPU
                    elif "-v100" in machine_type_name:
                        gpu_type = "NVIDIA Tesla V100"
                        gpu_memory = "16 GB"
                        if "standard-16-v100" in machine_type_name:
                            gpu_count = 2
                        elif "standard-32-v100" in machine_type_name:
                            gpu_count = 4
                        elif "standard-64-v100" in machine_type_name or "standard-96-v100" in machine_type_name:
                            gpu_count = 8
                        else:
                            gpu_count = 1
                    elif "-p100" in machine_type_name:
                        gpu_type = "NVIDIA Tesla P100"
                        gpu_memory = "16 GB"
                        if "standard-16-p100" in machine_type_name:
                            gpu_count = 2
                        elif "standard-32-p100" in machine_type_name:
                            gpu_count = 4
                        elif "standard-64-p100" in machine_type_name:
                            gpu_count = 8
                        else:
                            gpu_count = 1
            
            # Get vCPUs and memory information if not already set
            if vcpus == 0 or memory_gb == 0:
                machine_specs = get_machine_vcpus_memory(compute_client, project_id, primary_zone, machine_type_name)
                vcpus = machine_specs['vcpus']
                memory_gb = machine_specs['memory_gb']
            
            # Check if the machine type is available in this region
            # For GCP, we'll consider a machine type available if it exists in the API
            available = gpu_type != "Unknown" and gpu_count > 0
            
            # Get dynamic pricing information
            pricing = get_dynamic_pricing_info(billing_client, machine_type_name, gpu_type, region)
            
            # Create result entry
            result = {
                'machine_type': machine_type_name,
                'region': region,
                'available': available,
                'offered_in_region': True,
                'on_demand_price': pricing['on_demand'],
                'spot_available': pricing['spot'] is not None,
                'spot_price': pricing['spot'],
                'check_time': datetime.now().isoformat(),
                'gpu_type': gpu_type,
                'gpu_memory': gpu_memory,
                'gpu_count': gpu_count,
                'vcpus': vcpus,
                'memory_gb': memory_gb
            }
            
            results.append(result)
            
            if DEBUG:
                log_debug(f"Processed {machine_type_name} in {region}: "
                          f"Available: {available}, "
                          f"GPU Type: {gpu_type}, "
                          f"GPU Count: {gpu_count}, "
                          f"On-Demand: ${pricing['on_demand']}/hr, "
                          f"Spot: ${pricing['spot']}/hr")
    
    except Exception as e:
        log_error(f"Error checking region {region}: {e}")
    
    return results

def save_to_mysql(results):
    """Save the results to MySQL database with batch inserts"""
    if not results:
        log_info("No results to save to database")
        return
    
    try:
        # Connect to MySQL database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Prepare SQL statement
        sql = """
        INSERT INTO gcp_gpu_instances (
            machine_type, region, available, offered_in_region, 
            on_demand_price, spot_available, spot_price, gpu_type, 
            gpu_memory, gpu_count, vcpus, memory_gb, check_time
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Prepare batch data
        batch_data = []
        for result in results:
            # Convert ISO format datetime string to MySQL datetime
            check_time = datetime.fromisoformat(result.get('check_time')).strftime('%Y-%m-%d %H:%M:%S')
            
            # Round price values to 2 decimal places if they exist
            on_demand_price = result.get('on_demand_price')
            if on_demand_price is not None:
                on_demand_price = round(on_demand_price * 100) / 100
                
            spot_price = result.get('spot_price')
            if spot_price is not None:
                spot_price = round(spot_price * 100) / 100
            
            # Prepare values tuple
            values = (
                result.get('machine_type'),
                result.get('region'),
                result.get('available', False),
                result.get('offered_in_region', False),
                on_demand_price,
                result.get('spot_available'),
                spot_price,
                result.get('gpu_type'),
                result.get('gpu_memory'),
                result.get('gpu_count'),
                result.get('vcpus'),
                result.get('memory_gb'),
                check_time
            )
            batch_data.append(values)
        
        # Execute batch insert
        try:
            cursor.executemany(sql, batch_data)
            conn.commit()
            log_info(f"Successfully inserted {len(batch_data)} records into MySQL database in batch.")
        except mysql.connector.Error as e:
            log_error(f"Error inserting batch records: {e}")
            conn.rollback()
            
            # Fallback to individual inserts if batch fails
            records_inserted = 0
            for values in batch_data:
                try:
                    cursor.execute(sql, values)
                    records_inserted += 1
                except mysql.connector.Error as e:
                    log_error(f"Error inserting record: {e}")
                    conn.rollback()
            
            conn.commit()
            log_info(f"Inserted {records_inserted} records individually after batch failure.")
        
    except mysql.connector.Error as e:
        log_error(f"Database error: {e}")
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def is_default_price(machine_type, price):
    """Helper function to determine if a price comes from our default pricing"""
    if machine_type in DEFAULT_PRICING:
        # Check if the price matches our default pricing (within 1 cent)
        default_price = DEFAULT_PRICING[machine_type]['on_demand']
        return abs(price - default_price) < 0.01
    
    # Check similar machine type patterns
    for default_type, pricing in DEFAULT_PRICING.items():
        # Check if the machine type follows a similar pattern
        if (machine_type.startswith(default_type.split('-')[0]) and 
            '-'.join(machine_type.split('-')[1:]) == '-'.join(default_type.split('-')[1:])):
            default_price = pricing['on_demand']
            return abs(price - default_price) < 0.01
    
    return False

def main():
    """Main function to check GPU availability across regions with optimizations"""
    print("\n" + "="*80)
    print("GCP GPU CAPACITY CHECKER (OPTIMIZED)")
    print("="*80)
    
    # Verify that credentials are available
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        log_error("GCP_PROJECT_ID not found in environment variables.")
        log_error("Make sure you have created a .env file with GCP_PROJECT_ID.")
        return
    
    # Check if GOOGLE_APPLICATION_CREDENTIALS environment variable is set
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        log_error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        log_error("Make sure your GCP credentials are properly configured.")
        return
    
    # Verify database setup
    if not verify_database_table():
        log_error("Database setup failed. Exiting.")
        return
    
    # Keep track of timing
    start_time = time.time()
    all_results = []
    
    try:
        # Create a Compute Engine client
        compute_client = compute_v1.MachineTypesClient()
        
        # Create a Cloud Billing client for dynamic pricing
        billing_client = initialize_billing_client()
        if not billing_client:
            log_warning("Could not initialize Cloud Billing client. Will attempt dynamic pricing but may fall back to defaults.")
        else:
            log_info("Successfully initialized Cloud Billing client for dynamic pricing retrieval.")
        
        # Get all available regions (priority first)
        regions = get_all_regions(compute_client, project_id)
        if not regions:
            log_warning("Could not get regions list. Using default regions.")
            regions = GCP_REGIONS
        
        print(f"Checking GPU availability across {len(regions)} GCP regions (priority first)...")
        
        # Prepare region and client info for parallel processing
        region_infos = [(region, project_id, compute_client, billing_client) for region in regions]
        
        # Use ThreadPoolExecutor to check regions in parallel with increased workers
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all region checks
            future_to_region = {
                executor.submit(check_gpu_availability_for_region, region_info): region_info[0]
                for region_info in region_infos
            }
            
            # Process completed futures
            for future in as_completed(future_to_region.keys()):
                region = future_to_region[future]
                try:
                    region_result = future.result(timeout=REGION_CHECK_TIMEOUT)
                    all_results.extend(region_result)
                    log_info(f"Completed check for region {region}")
                except Exception as e:
                    log_error(f"Region check for {region} failed: {e}")
        
        # Generate a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # # Write results to a JSON file (optional)
        # if DEBUG:
        #     filename = f"gcp_gpu_availability_{timestamp}.json"
        #     with open(filename, 'w') as f:
        #         json.dump(all_results, f, indent=2)
        
        # Save results to MySQL database with batch inserts
        save_to_mysql(all_results)
        
        # Print summary
        available_instances = [r for r in all_results if r.get('available', False)]
        offered_instances = [r for r in all_results if r.get('offered_in_region', False)]
        
        # Get pricing statistics
        dynamic_priced_instances = [r for r in all_results if r.get('on_demand_price') is not None and not is_default_price(r.get('machine_type'), r.get('on_demand_price'))]
        default_priced_instances = [r for r in all_results if r.get('on_demand_price') is not None and is_default_price(r.get('machine_type'), r.get('on_demand_price'))]
        spot_instances = [r for r in all_results if r.get('spot_price') is not None]
        
        print(f"\nCompleted in {round(time.time() - start_time, 2)} seconds.")
        print(f"Found {len(available_instances)} available GPU instances out of {len(all_results)} total checks.")
        print(f"VM types offered in regions: {len(offered_instances)}")
        print(f"Retrieved dynamic pricing for {len(dynamic_priced_instances)} VMs")
        print(f"Using fallback pricing for {len(default_priced_instances)} VMs")
        print(f"Retrieved spot pricing for {len(spot_instances)} VMs")
        
        if DEBUG:
            print(f"Debug JSON saved to {filename}")
        
        # Print a quick availability summary
        print("\nAvailability Summary (showing up to 20 entries):")
        summary = []
        
        # Focus on high-end GPU types for the summary
        high_end_gpus = ["a100", "h100", "h200", "a10", "gh200", "gb200", "l4"]
        
        for result in all_results:
            if result.get('offered_in_region', False) and any(gpu in result.get('gpu_type', '').lower() for gpu in high_end_gpus):
                status = "AVAILABLE" if result.get('available', False) else "NOT AVAILABLE"
                
                price = result.get('on_demand_price')
                price_str = f"${price}/hr" if price is not None else "Unknown"
                
                spot = result.get('spot_price')
                spot_str = f"${spot}/hr" if spot is not None else "No spot"
                
                summary.append({
                    'key': f"{result['machine_type']} ({result['gpu_type']}) in {result['region']}",
                    'status': status,
                    'price': price_str,
                    'spot': spot_str
                })
        
        # Sort by machine type and region
        sorted_summary = sorted(summary, key=lambda x: x['key'])
        
        # Print the first 20 entries
        for entry in sorted_summary[:20]:
            print(f"{entry['key']}: {entry['status']}, Price: {entry['price']}, Spot: {entry['spot']}")
        
        if len(sorted_summary) > 20:
            print(f"... and {len(sorted_summary) - 20} more high-end GPU entries")
            
    except KeyboardInterrupt:
        print("\nOperation interrupted by user. Saving partial results...")
        
        if all_results:
            # Save partial results to MySQL database
            save_to_mysql(all_results)
            print("Partial results saved to MySQL database.")
        
        return
    except Exception as e:
        log_error(f"Unexpected error in main execution: {e}")
        return

if __name__ == "__main__":
    main()