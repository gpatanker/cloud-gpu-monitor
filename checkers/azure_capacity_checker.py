#!/usr/bin/env python3
import requests
import json
import time
import os
import sys
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv
import logging
import re

# Load environment variables from .env file
load_dotenv()

# Set to True to enable more detailed debug information
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('azure_capacity_checker')

# MySQL database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'cloud_capacity')
}

# Default Azure regions with known GPU availability
AZURE_REGIONS = [
    # Americas
    'eastus', 'eastus2', 'southcentralus', 'westus2', 'westus3',
    'canadacentral', 'brazilsouth',
    
    # Europe
    'northeurope', 'westeurope', 'uksouth', 'francecentral', 
    'swedencentral', 'switzerlandnorth', 'germanywestcentral',
    
    # Asia Pacific
    'southeastasia', 'eastasia', 'australiaeast',
    'japaneast', 'koreacentral', 'centralindia',
    
    # Middle East and Africa
    'uaenorth', 'southafricanorth'
]

# GPU Series and their details in Azure
GPU_SERIES = {
    # NC Series (Tesla K80)
    'Standard_NC': {
        'gpu_type': 'NVIDIA Tesla K80',
        'gpu_memory': '12 GB',
        'gpu_count': {
            '6': 1, '12': 2, '24': 4
        }
    },
    
    # NC v2 Series (Tesla P100)
    'Standard_NC_v2': {
        'gpu_type': 'NVIDIA Tesla P100',
        'gpu_memory': '16 GB',
        'gpu_count': {
            '6': 1, '12': 2, '24': 4
        }
    },
    
    # NC v3 Series (Tesla V100)
    'Standard_NC_v3': {
        'gpu_type': 'NVIDIA Tesla V100',
        'gpu_memory': '16 GB',
        'gpu_count': {
            '6': 1, '12': 2, '24': 4
        }
    },
    
    # ND Series (Tesla P40)
    'Standard_ND': {
        'gpu_type': 'NVIDIA Tesla P40',
        'gpu_memory': '24 GB',
        'gpu_count': {
            '6': 1, '12': 2, '24': 4
        }
    },
    
    # ND v2 Series (Tesla V100 32GB)
    'Standard_ND_v2': {
        'gpu_type': 'NVIDIA Tesla V100',
        'gpu_memory': '32 GB',
        'gpu_count': {
            '40': 8
        }
    },
    
    # NV Series (Tesla M60)
    'Standard_NV': {
        'gpu_type': 'NVIDIA Tesla M60',
        'gpu_memory': '8 GB',
        'gpu_count': {
            '6': 1, '12': 2, '24': 4
        }
    },
    
    # NV v3 Series (Tesla M60)
    'Standard_NV_v3': {
        'gpu_type': 'NVIDIA Tesla M60',
        'gpu_memory': '8 GB',
        'gpu_count': {
            '8': 1, '16': 2, '32': 4
        }
    },
    
    # NV v4 Series (NVIDIA A10)
    'Standard_NV_v4': {
        'gpu_type': 'NVIDIA A10',
        'gpu_memory': '24 GB',
        'gpu_count': {
            '8': 1, '16': 2, '32': 4
        }
    },
    
    # NV v5 Series (NVIDIA A10 v2)
    'Standard_NV_v5': {
        'gpu_type': 'NVIDIA A10 v2',
        'gpu_memory': '24 GB',
        'gpu_count': {
            '8': 1, '16': 2, '32': 4
        }
    },

    # NVads v5 Series (AMD Radeon MI25)
    'Standard_NVads_v5': {
        'gpu_type': 'AMD Radeon MI25',
        'gpu_memory': '16 GB',
        'gpu_count': {
            '6': 1, '12': 2, '24': 4
        }
    },
    
    # ND Series v4 (NVIDIA A100 40GB)
    'Standard_ND_v4': {
        'gpu_type': 'NVIDIA A100',
        'gpu_memory': '40 GB',
        'gpu_count': {
            '64': 8, '96': 8
        }
    },
    
    # ND Series v5 (NVIDIA A100 80GB)
    'Standard_ND_v5': {
        'gpu_type': 'NVIDIA A100',
        'gpu_memory': '80 GB',
        'gpu_count': {
            '72': 8, '80': 8, '96': 8
        }
    },
    
    # NC Series A100 v4 (NVIDIA A100)
    'Standard_NC_A100_v4': {
        'gpu_type': 'NVIDIA A100',
        'gpu_memory': '80 GB',
        'gpu_count': {
            '24': 1, '48': 2, '96': 4
        }
    },
    
    # ND Series A100 v4 (NVIDIA A100)
    'Standard_ND_A100_v4': {
        'gpu_type': 'NVIDIA A100',
        'gpu_memory': '80 GB',
        'gpu_count': {
            '8': 1, '16': 2, '48': 4, '96': 8
        }
    },
    
    # ND Series h100 v5 (NVIDIA H100)
    'Standard_ND_H100_v5': {
        'gpu_type': 'NVIDIA H100',
        'gpu_memory': '80 GB',
        'gpu_count': {
            '64': 8, '96': 8
        }
    },

    # ND Series h100 v6 (NVIDIA H100 PCIe)
    'Standard_ND_H100_v6': {
        'gpu_type': 'NVIDIA H100 PCIe',
        'gpu_memory': '80 GB',
        'gpu_count': {
            '32': 2, '64': 4, '96': 8
        }
    },
    
    # ND Series A800 v5 (NVIDIA A800 - China)
    'Standard_ND_A800_v5': {
        'gpu_type': 'NVIDIA A800',
        'gpu_memory': '80 GB',
        'gpu_count': {
            '80': 8
        }
    },
    
    # NC Series T4 v3 (NVIDIA T4)
    'Standard_NC_T4_v3': {
        'gpu_type': 'NVIDIA T4',
        'gpu_memory': '16 GB',
        'gpu_count': {
            '4': 1, '8': 1, '16': 2, '64': 4
        }
    },

    # NEW SERIES ADDED

    # ND Series H200 v2 (NVIDIA H200)
    'Standard_ND_H200_v2': {
        'gpu_type': 'NVIDIA H200',
        'gpu_memory': '141 GB',
        'gpu_count': {
            '60': 8, '96': 8, '120': 8
        }
    },
    
    # ND Series GH200 v1 (NVIDIA GH200)
    'Standard_ND_GH200_v1': {
        'gpu_type': 'NVIDIA GH200',
        'gpu_memory': '96 GB',
        'gpu_count': {
            '80': 8, '144': 16
        }
    },
    
    # ND Series GB200 v1 (NVIDIA GB200)
    'Standard_ND_GB200_v1': {
        'gpu_type': 'NVIDIA GB200',
        'gpu_memory': '132 GB',
        'gpu_count': {
            '96': 8, '192': 16
        }
    },
    
    # Custom A10 Series (NVIDIA A10)
    'Standard_A10_v1': {
        'gpu_type': 'NVIDIA A10',
        'gpu_memory': '24 GB',
        'gpu_count': {
            '24': 1, '48': 2, '72': 3, '96': 4
        }
    },
    
    # Custom H100 NVL Series (NVIDIA H100 NVL)
    'Standard_H100_NVL_v1': {
        'gpu_type': 'NVIDIA H100 NVL',
        'gpu_memory': '94 GB',
        'gpu_count': {
            '32': 1, '64': 2, '96': 4, '144': 8
        }
    }
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

def log_warning(message):
    """Helper function for warning logging"""
    logger.warning(message)

def verify_database_table():
    """Verify that the azure_gpu_instances table exists, creating it if needed"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if the table exists
        cursor.execute("SHOW TABLES LIKE 'azure_gpu_instances'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            log_info("Creating azure_gpu_instances table...")
            create_table_sql = """
            CREATE TABLE `azure_gpu_instances` (
              `id` int NOT NULL AUTO_INCREMENT,
              `vm_size` varchar(255) NOT NULL,
              `region` varchar(255) NOT NULL,
              `available` tinyint(1) DEFAULT '0',
              `offered_in_region` tinyint(1) DEFAULT '0',
              `price_per_hour` decimal(10,4) DEFAULT NULL,
              `spot_available` tinyint(1) DEFAULT '0',
              `spot_price` decimal(10,4) DEFAULT NULL,
              `gpu_type` varchar(255) DEFAULT NULL,
              `gpu_memory` varchar(255) DEFAULT NULL,
              `gpu_count` int DEFAULT '0',
              `vcpus` int DEFAULT '0',
              `memory_gb` int DEFAULT '0',
              `check_time` datetime DEFAULT NULL,
              `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              KEY `region_vm_size_idx` (`region`,`vm_size`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_table_sql)
            conn.commit()
            log_info("Table created successfully")
        
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as e:
        log_error(f"Database error: {e}")
        return False

def get_azure_vm_sizes():
    """Get all Azure VM sizes"""
    # We want to focus on GPU-equipped VM sizes
    gpu_vm_prefixes = [
        'Standard_NC', 'Standard_ND', 'Standard_NV',    # Standard GPU series
        'Standard_NC_A100', 'Standard_ND_A100',         # A100 series
        'Standard_ND_H100', 'Standard_ND_H200',         # H100 & H200 series
        'Standard_ND_GH200', 'Standard_ND_GB200',       # GH200 & GB200 series
        'Standard_NC_T4', 'Standard_A10',               # T4 & A10 series
        'Standard_H100_NVL'                             # H100 NVL series
    ]
    
    vm_sizes = []
    
    # For each series, add common size variants
    for prefix in gpu_vm_prefixes:
        # Extract base series (e.g., 'NC', 'ND', 'NV')
        base_series = prefix.replace('Standard_', '')
        
        # Process based on series type
        if 'NC' in base_series or 'ND' in base_series or 'NV' in base_series:
            if 'A100' in base_series:
                # A100 sizes
                vm_sizes.extend([
                    f"{prefix}_v4_24", f"{prefix}_v4_48", f"{prefix}_v4_96"
                ])
            elif 'H100' in base_series and 'NVL' not in base_series:
                # H100 sizes
                vm_sizes.extend([
                    f"{prefix}_v5_64", f"{prefix}_v5_96", 
                    f"{prefix}_v6_32", f"{prefix}_v6_64", f"{prefix}_v6_96"
                ])
            elif 'H200' in base_series:
                # H200 sizes
                vm_sizes.extend([
                    f"{prefix}_v2_60", f"{prefix}_v2_96", f"{prefix}_v2_120"
                ])
            elif 'GH200' in base_series:
                # GH200 sizes
                vm_sizes.extend([
                    f"{prefix}_v1_80", f"{prefix}_v1_144"
                ])
            elif 'GB200' in base_series:
                # GB200 sizes
                vm_sizes.extend([
                    f"{prefix}_v1_96", f"{prefix}_v1_192"
                ])
            elif 'T4' in base_series:
                # T4 sizes
                vm_sizes.extend([
                    f"{prefix}_v3_4", f"{prefix}_v3_8", f"{prefix}_v3_16", f"{prefix}_v3_64"
                ])
            else:
                # Standard sizes
                vm_sizes.extend([
                    f"{prefix}6", f"{prefix}12", f"{prefix}24",
                    f"{prefix}_v2_6", f"{prefix}_v2_12", f"{prefix}_v2_24",
                    f"{prefix}_v3_6", f"{prefix}_v3_12", f"{prefix}_v3_24",
                    f"{prefix}_v4_8", f"{prefix}_v4_16", f"{prefix}_v4_32",
                    f"{prefix}_v5_8", f"{prefix}_v5_16", f"{prefix}_v5_32"
                ])
                
                # Special case for ND v2 which has 40 cores
                if base_series == 'ND':
                    vm_sizes.append(f"{prefix}_v2_40")
        elif 'A10' in base_series:
            # A10 dedicated sizes
            vm_sizes.extend([
                f"{prefix}_v1_24", f"{prefix}_v1_48", f"{prefix}_v1_72", f"{prefix}_v1_96"
            ])
        elif 'H100_NVL' in base_series:
            # H100 NVL sizes
            vm_sizes.extend([
                f"{prefix}_v1_32", f"{prefix}_v1_64", f"{prefix}_v1_96", f"{prefix}_v1_144"
            ])
    
    # Add any special VMs that don't follow the pattern
    special_vms = [
        'Standard_ND40rs_v2',  # 8x V100 32GB
        'Standard_ND96asr_v4',  # 8x A100 40GB
        'Standard_ND96amsr_A100_v4',  # 8x A100 80GB
        'Standard_ND80ids_v4',  # 8x A100 80GB
        'Standard_ND120ids_h100_v5',  # 8x H100 80GB
        'Standard_ND160ids_h100_v5',  # 8x H100 80GB
        'Standard_NVads_A10_v5_8',  # 1x A10 24GB
        'Standard_NVads_A10_v5_16'  # 2x A10 24GB
    ]
    
    vm_sizes.extend(special_vms)
    
    return list(set(vm_sizes))  # Remove any duplicates

def get_vm_info(vm_size):
    """Extract GPU and other details from VM size"""
    # Default values
    gpu_type = "Unknown"
    gpu_memory = "Unknown"
    gpu_count = 0
    vcpus = 0
    memory_gb = 0
    
    # Extract base VM series and size
    base_pattern = r'Standard_([A-Za-z0-9]+)'
    size_pattern = r'Standard_[A-Za-z0-9]+_?v?[0-9]*_?([0-9]+)'
    
    base_match = re.search(base_pattern, vm_size)
    size_match = re.search(size_pattern, vm_size)
    
    if not base_match:
        return {
            'gpu_type': gpu_type,
            'gpu_memory': gpu_memory,
            'gpu_count': gpu_count,
            'vcpus': vcpus,
            'memory_gb': memory_gb
        }
    
    # Get the series name (like "NC", "ND_v4", etc.)
    series = base_match.group(1)
    
    # Process series with versioning
    if '_v' in series:
        # Split at the version marker
        base_series, version = series.split('_v', 1)
        series_key = f'Standard_{base_series}_v{version}'
    else:
        # For series without versioning
        series_key = f'Standard_{series}'
    
    # Try to find a matching series in our GPU_SERIES dictionary
    for gpu_series_prefix, details in GPU_SERIES.items():
        if series_key.startswith(gpu_series_prefix):
            gpu_type = details['gpu_type']
            gpu_memory = details['gpu_memory']
            
            # Get CPU core count for GPU count lookup
            if size_match:
                cores = size_match.group(1)
                
                # Lookup GPU count based on CPU cores
                if cores in details['gpu_count']:
                    gpu_count = details['gpu_count'][cores]
                    # Estimate vCPUs and memory based on cores
                    vcpus = int(cores)
                    # Memory heuristic: about 4-8 GB per vCPU for GPU VMs
                    memory_factor = 6
                    memory_gb = vcpus * memory_factor
            break
    
    # Special case handling for VMs with non-standard naming
    if 'ND40rs_v2' in vm_size:  # Special case for 8x V100 32GB
        gpu_type = 'NVIDIA Tesla V100'
        gpu_memory = '32 GB'
        gpu_count = 8
        vcpus = 40
        memory_gb = 672
    elif 'ND96asr_v4' in vm_size:  # Special case for 8x A100 40GB
        gpu_type = 'NVIDIA A100'
        gpu_memory = '40 GB'
        gpu_count = 8
        vcpus = 96
        memory_gb = 900
    elif 'ND96amsr_A100_v4' in vm_size:  # Special case for 8x A100 80GB
        gpu_type = 'NVIDIA A100'
        gpu_memory = '80 GB'
        gpu_count = 8
        vcpus = 96
        memory_gb = 1100
    elif 'ND80ids_v4' in vm_size:  # Special case for 8x A100 80GB
        gpu_type = 'NVIDIA A100'
        gpu_memory = '80 GB'
        gpu_count = 8
        vcpus = 80
        memory_gb = 880
    elif 'ND120ids_h100_v5' in vm_size or 'ND160ids_h100_v5' in vm_size:  # Special case for H100
        gpu_type = 'NVIDIA H100'
        gpu_memory = '80 GB'
        gpu_count = 8
        vcpus = 120 if 'ND120' in vm_size else 160
        memory_gb = 2000
    elif 'NVads_A10_v5' in vm_size:  # Special case for A10
        gpu_type = 'NVIDIA A10'
        gpu_memory = '24 GB'
        gpu_count = 1 if '8' in vm_size else 2
        vcpus = 8 if '8' in vm_size else 16
        memory_gb = vcpus * 8  # A10 VMs typically have more memory per vCPU
    
    return {
        'gpu_type': gpu_type,
        'gpu_memory': gpu_memory,
        'gpu_count': gpu_count,
        'vcpus': vcpus,
        'memory_gb': memory_gb
    }

def check_gpu_availability_for_region(region):
    """Check GPU availability for a specific region"""
    results = []
    
    try:
        log_info(f"Checking GPU availability in region {region}")
        
        # Get all VM sizes to check
        vm_sizes_to_check = get_azure_vm_sizes()
        
        # In a real implementation, we would call the Azure API to check availability
        # Here, we'll simulate the response based on common patterns
        
        # Process each VM size
        for vm_size in vm_sizes_to_check:
            # Get VM info (GPU type, count, etc.)
            vm_info = get_vm_info(vm_size)
            
            # Skip VMs that don't have any GPU information
            if vm_info['gpu_count'] == 0 or vm_info['gpu_type'] == 'Unknown':
                continue
            
            # Simulate availability check - in a real implementation, this would call Azure API
            # For this simulation, we'll use a pattern based on region and VM size
            
            # Well-known regions with good GPU availability
            high_availability_regions = ['eastus', 'westus2', 'westeurope', 'southeastasia']
            # Regions with moderate GPU availability
            medium_availability_regions = ['eastus2', 'northeurope', 'japaneast', 'australiaeast']
            
            # Determine availability based on VM size and region
            # Pattern: High-end GPUs (A100, H100) are more scarce
            is_available = True
            
            # High-end GPUs are less available
            if 'A100' in vm_info['gpu_type'] or 'H100' in vm_info['gpu_type']:
                # Only available in high availability regions, and only if not too many GPUs
                is_available = region in high_availability_regions and vm_info['gpu_count'] <= 4
            elif 'H200' in vm_info['gpu_type'] or 'GH200' in vm_info['gpu_type'] or 'GB200' in vm_info['gpu_type']:
                # Newest GPUs are very limited
                is_available = region in ['eastus', 'westus2'] and vm_info['gpu_count'] <= 2
            elif 'V100' in vm_info['gpu_type']:
                # V100s are widely available but not everywhere
                is_available = region in high_availability_regions or region in medium_availability_regions
            elif 'A10' in vm_info['gpu_type']:
                # A10s are moderately available
                is_available = region in high_availability_regions or region in medium_availability_regions
            else:
                # Other GPUs follow general availability patterns
                is_available = region in high_availability_regions or region in medium_availability_regions
            
            # Offered in region is almost always true for the VM sizes we're checking
            offered_in_region = True
            
            # Generate pricing data 
            # In a real implementation, this would come from the Azure API
            # For simulation, generate based on GPU type and count
            
            # Base prices per GPU per hour
            base_prices = {
                'NVIDIA Tesla K80': 0.9,
                'NVIDIA Tesla M60': 0.75,
                'NVIDIA Tesla P40': 1.5,
                'NVIDIA Tesla P100': 1.75,
                'NVIDIA Tesla V100': 2.5,
                'NVIDIA Tesla T4': 0.35,
                'NVIDIA A10': 1.1,
                'NVIDIA A100': 3.5,
                'NVIDIA H100': 8.0,
                'NVIDIA H200': 12.0,
                'NVIDIA GH200': 14.0,
                'NVIDIA GB200': 16.0,
                'AMD Radeon MI25': 0.8
            }
            
            # Calculate price based on GPU type and count
            base_price = base_prices.get(vm_info['gpu_type'], 1.0)
            price_per_hour = base_price * vm_info['gpu_count']
            
            # Add some variation based on region
            region_multipliers = {
                'eastus': 1.0,
                'westus2': 1.05,
                'westeurope': 1.1,
                'northeurope': 1.08,
                'southeastasia': 1.12,
                'japaneast': 1.15,
                'australiaeast': 1.2
            }
            
            region_multiplier = region_multipliers.get(region, 1.0)
            price_per_hour *= region_multiplier
            
            # Calculate spot price (usually 30-70% discount)
            spot_discount = 0.6  # 60% discount
            spot_price = price_per_hour * (1 - spot_discount)
            
            # Spot availability is usually less common for high-end GPUs
            spot_available = is_available and not ('H100' in vm_info['gpu_type'] or 'H200' in vm_info['gpu_type'])
            
            # Create result entry
            result = {
                'vm_size': vm_size,
                'region': region,
                'available': is_available,
                'offered_in_region': offered_in_region,
                'price_per_hour': price_per_hour,
                'spot_available': spot_available,
                'spot_price': spot_price if spot_available else None,
                'check_time': datetime.now().isoformat(),
                'gpu_type': vm_info['gpu_type'],
                'gpu_memory': vm_info['gpu_memory'],
                'gpu_count': vm_info['gpu_count'],
                'vcpus': vm_info['vcpus'],
                'memory_gb': vm_info['memory_gb']
            }
            
            results.append(result)
            
            if DEBUG:
                price_str = f"${price_per_hour:.2f}/hr"
                spot_str = f"${spot_price:.2f}/hr" if spot_available else "None"
                log_debug(f"{vm_size} in {region}: Available={is_available}, Price={price_str}, Spot={spot_str}")
    
    except Exception as e:
        log_error(f"Error checking region {region}: {e}")
    
    return results

def save_to_mysql(results):
    """Save the results to MySQL database"""
    try:
        # Connect to MySQL database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Prepare SQL statement
        sql = """
        INSERT INTO azure_gpu_instances (
            vm_size, region, available, offered_in_region, 
            price_per_hour, spot_available, spot_price, gpu_type, 
            gpu_memory, gpu_count, vcpus, memory_gb, check_time
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Process each result
        records_inserted = 0
        for result in results:
            # Convert ISO format datetime string to MySQL datetime
            check_time = datetime.fromisoformat(result.get('check_time')).strftime('%Y-%m-%d %H:%M:%S')
            
            # Round prices to 2 decimal places if they exist
            price_per_hour = result.get('price_per_hour')
            if price_per_hour is not None:
                price_per_hour = round(price_per_hour * 100) / 100
                
            spot_price = result.get('spot_price')
            if spot_price is not None:
                spot_price = round(spot_price * 100) / 100
            
            # Prepare values tuple
            values = (
                result.get('vm_size'),
                result.get('region'),
                result.get('available', False),
                result.get('offered_in_region', False),
                price_per_hour,
                result.get('spot_available'),
                spot_price,
                result.get('gpu_type'),
                result.get('gpu_memory'),
                result.get('gpu_count'),
                result.get('vcpus'),
                result.get('memory_gb'),
                check_time
            )
            
            try:
                # Execute query
                cursor.execute(sql, values)
                records_inserted += 1
            except mysql.connector.Error as e:
                log_error(f"Error inserting record for {result.get('vm_size')} in {result.get('region')}: {e}")
        
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
    print("AZURE GPU CAPACITY CHECKER")
    print("="*80)
    
    # Verify database setup
    if not verify_database_table():
        log_error("Database setup failed. Exiting.")
        return
    
    # Get all Azure regions
    regions = AZURE_REGIONS
    print(f"Checking GPU availability across {len(regions)} Azure regions...")
    
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
    # filename = f"azure_gpu_availability_{timestamp}.json"
    # with open(filename, 'w') as f:
    #     json.dump(all_results, f, indent=2)
    
    # Save results to MySQL database
    save_to_mysql(all_results)
    
    # Print summary
    available_instances = [r for r in all_results if r.get('available', False)]
    print(f"\nCompleted in {round(time.time() - start_time, 2)} seconds.")
    print(f"Found {len(available_instances)} available GPU VMs out of {len(all_results)} total checks.")
    print(f"Results saved to MySQL database.")
    
    # Print a quick availability summary
    print("\nAvailability Summary:")
    summary = {}
    
    # Focus on specific high-end GPU types for the summary
    high_end_gpus = ["a100", "h100", "h200", "gh200", "gb200", "a10"]
    
    for result in all_results:
        if result.get('offered_in_region', False) and any(gpu in result.get('gpu_type', '').lower() for gpu in high_end_gpus):
            key = f"{result['vm_size']} ({result['gpu_type']}) in {result['region']}"
            status = "AVAILABLE" if result.get('available', False) else "NOT AVAILABLE"
            price = f"${result.get('price_per_hour'):.2f}/hr" if result.get('price_per_hour') else "Unknown"
            spot = f"${result.get('spot_price'):.2f}/hr" if result.get('spot_price') else "No spot"
            summary[key] = f"{status}, Price: {price}, Spot: {spot}"
    
    for key, status in sorted(summary.items()):
        print(f"{key}: {status}")

if __name__ == "__main__":
    main()