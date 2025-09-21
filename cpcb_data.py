import requests
import json
import csv
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional
from collections import defaultdict
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('air_quality_fetch.log'),
        logging.StreamHandler()
    ]
)

class DelhiAirQualityFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.data.gov.in/resource"
        self.resource_id = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"  # Air Quality Index resource ID
        self.headers = {
            'User-Agent': 'Delhi-Air-Quality-Fetcher/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        self.raw_data = []
        self.aggregated_data = []
        
    def construct_api_url(self, limit: int = 100, offset: int = 0, filters: Dict = None) -> str:
        """Construct the API URL with parameters"""
        url = f"{self.base_url}/{self.resource_id}"
        params = {
            'api-key': self.api_key,
            'format': 'json',
            'limit': limit,
            'offset': offset
        }
        
        # Add filters for Delhi data
        if filters:
            for key, value in filters.items():
                params[f'filters[{key}]'] = value
        
        # Convert params to query string
        query_params = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{url}?{query_params}"
    
    def fetch_data_batch(self, limit: int = 100, offset: int = 0) -> Optional[Dict]:
        """Fetch a batch of data from the API"""
        try:
            # Filter for Delhi data
            filters = {
                'state': 'Delhi',
                'city': 'Delhi'
            }
            
            url = self.construct_api_url(limit=limit, offset=offset, filters=filters)
            logging.info(f"Fetching data from offset {offset}, limit {limit}")
            
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error at offset {offset}: {e}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error at offset {offset}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error at offset {offset}: {e}")
            return None
    
    def normalize_pollutant_name(self, pollutant_id: str) -> str:
        """Normalize pollutant names to standard format"""
        pollutant_map = {
            'PM2.5': 'pm25',
            'PM10': 'pm10',
            'O3': 'o3',
            'Ozone': 'o3',
            'NO2': 'no2',
            'SO2': 'so2',
            'CO': 'co',
            'NH3': 'nh3',
            'Ammonia': 'nh3',
            'Pb': 'pb',
            'Lead': 'pb',
            'Benzene': 'benzene',
            'Toluene': 'toluene',
            'Xylene': 'xylene',
            'MP-Xylene': 'mp_xylene',
            'Eth-Benzene': 'eth_benzene'
        }
        return pollutant_map.get(pollutant_id, pollutant_id.lower().replace('-', '_').replace(' ', '_'))
    
    def process_record(self, record: Dict) -> Dict:
        """Process and standardize a single record"""
        processed = {
            'timestamp': datetime.now().isoformat(),
            'fetch_date': datetime.now().strftime('%Y-%m-%d'),
            'station_id': record.get('id', ''),
            'station_name': record.get('station', ''),
            'city': record.get('city', 'Delhi'),
            'state': record.get('state', 'Delhi'),
            'country': record.get('country', 'India'),
            'latitude': record.get('latitude', ''),
            'longitude': record.get('longitude', ''),
            'last_update': record.get('last_update', ''),
            'pollutant_id': record.get('pollutant_id', ''),
            'pollutant_min': record.get('min_value', ''),
            'pollutant_max': record.get('max_value', ''),
            'pollutant_avg': record.get('avg_value', ''),
            'pollutant_unit': record.get('pollutant_unit', ''),
        }
        
        return processed
    
    def aggregate_by_station(self, raw_data: List[Dict]) -> List[Dict]:
        """Aggregate pollutant data by station - only include pollutants with actual data"""
        station_data = defaultdict(dict)
        
        # Define the specific pollutants we want (based on common air quality standards)
        target_pollutants = ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'nh3']
        
        for record in raw_data:
            # Only process records with actual pollutant data
            if not record['pollutant_id'] or not record['pollutant_avg']:
                continue
                
            station_key = (record['station_name'], record['latitude'], record['longitude'])
            
            if station_key not in station_data:
                # Initialize station record
                station_data[station_key] = {
                    'station_name': record['station_name'].strip(),
                    'city': record['city'].strip(),
                    'state': record['state'].strip(),
                    'latitude': float(record['latitude']) if record['latitude'] else None,
                    'longitude': float(record['longitude']) if record['longitude'] else None,
                    'last_update': record['last_update'].strip() if record['last_update'] else '',
                    'pollutants': {}
                }
            
            # Add pollutant data only if it's in our target list
            pollutant_name = self.normalize_pollutant_name(record['pollutant_id'])
            if pollutant_name in target_pollutants:
                # Only add if we have actual numeric data
                try:
                    min_val = float(record['pollutant_min']) if record['pollutant_min'] else None
                    max_val = float(record['pollutant_max']) if record['pollutant_max'] else None
                    avg_val = float(record['pollutant_avg']) if record['pollutant_avg'] else None
                    
                    if avg_val is not None:  # Only include if we have at least average value
                        station_data[station_key]['pollutants'][pollutant_name] = {
                            'min': min_val,
                            'max': max_val,
                            'avg': avg_val,
                            'unit': record['pollutant_unit'].strip() if record['pollutant_unit'] else 'µg/m³'
                        }
                except (ValueError, TypeError):
                    continue  # Skip invalid numeric data
        
        # Convert to final format - only stations with pollutant data
        aggregated = []
        for station_info in station_data.values():
            if not station_info['pollutants']:  # Skip stations with no pollutant data
                continue
                
            # Build clean record with only available pollutants
            clean_record = {
                'station_name': station_info['station_name'],
                'city': station_info['city'],
                'state': station_info['state'],
                'latitude': station_info['latitude'],
                'longitude': station_info['longitude'],
                'last_update': station_info['last_update'],
                'total_pollutants_monitored': len(station_info['pollutants'])
            }
            
            # Add pollutant data dynamically (only pollutants with data)
            for pollutant, data in station_info['pollutants'].items():
                clean_record[f'{pollutant}_min'] = data['min']
                clean_record[f'{pollutant}_max'] = data['max']
                clean_record[f'{pollutant}_avg'] = data['avg']
                clean_record[f'{pollutant}_unit'] = data['unit']
            
            aggregated.append(clean_record)
        
        return aggregated
    
    def fetch_all_data(self, max_records: int = 10000) -> List[Dict]:
        """Fetch all available data with pagination"""
        offset = 0
        limit = 100  # API limit per request
        total_fetched = 0
        
        logging.info("Starting data fetch for Delhi air quality...")
        
        while total_fetched < max_records:
            # Add delay to be respectful to the API
            if offset > 0:
                time.sleep(1)
            
            batch_data = self.fetch_data_batch(limit=limit, offset=offset)
            
            if not batch_data:
                logging.warning(f"No data received at offset {offset}")
                break
                
            if 'records' not in batch_data:
                logging.warning("No 'records' field in response")
                break
                
            records = batch_data['records']
            
            if not records:
                logging.info("No more records available")
                break
            
            # Process each record
            for record in records:
                processed_record = self.process_record(record)
                self.raw_data.append(processed_record)
                total_fetched += 1
                
                if total_fetched >= max_records:
                    break
            
            logging.info(f"Fetched {len(records)} records, total: {total_fetched}")
            offset += limit
            
            # If we got fewer records than the limit, we've reached the end
            if len(records) < limit:
                break
        
        # Aggregate data by station
        logging.info("Aggregating data by station...")
        self.aggregated_data = self.aggregate_by_station(self.raw_data)
        
        logging.info(f"Total raw records fetched: {len(self.raw_data)}")
        logging.info(f"Total stations aggregated: {len(self.aggregated_data)}")
        
        return self.aggregated_data
    
    def save_to_csv(self, filename: str = None) -> str:
        """Save clean aggregated data to CSV file"""
        if not self.aggregated_data:
            logging.warning("No aggregated data to save")
            return None
            
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"delhi_air_quality_clean_{timestamp}.csv"
        
        try:
            df = pd.DataFrame(self.aggregated_data)
            
            # Remove any completely empty columns
            df = df.dropna(axis=1, how='all')
            
            # Sort columns for better readability
            base_cols = ['station_name', 'city', 'state', 'latitude', 'longitude', 
                        'last_update', 'total_pollutants_monitored']
            
            pollutant_cols = [col for col in df.columns if col not in base_cols]
            pollutant_cols.sort()
            
            final_cols = base_cols + pollutant_cols
            df = df[final_cols]
            
            # Save to CSV
            df.to_csv(filename, index=False)
            
            logging.info(f"Clean data saved to {filename}")
            logging.info(f"Total stations with data: {len(df)}")
            logging.info(f"Total columns: {len(df.columns)}")
            
            # Print column summary
            pollutants_found = set()
            for col in df.columns:
                for pollutant in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'nh3']:
                    if col.startswith(pollutant + '_'):
                        pollutants_found.add(pollutant.upper())
            
            logging.info(f"Pollutants with data: {', '.join(sorted(pollutants_found))}")
            
            return filename
            
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")
            return None
    
    def save_to_json(self, filename: str = None) -> str:
        """Save clean data to JSON file"""
        if not self.aggregated_data:
            logging.warning("No data to save")
            return None
            
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"delhi_air_quality_clean_{timestamp}.json"
        
        try:
            # Get pollutants that actually have data
            pollutants_with_data = set()
            for record in self.aggregated_data:
                for key in record.keys():
                    for pollutant in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'nh3']:
                        if key.startswith(pollutant + '_avg'):
                            pollutants_with_data.add(pollutant.upper())
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'metadata': {
                        'fetch_timestamp': datetime.now().isoformat(),
                        'total_stations': len(self.aggregated_data),
                        'pollutants_available': sorted(list(pollutants_with_data)),
                        'source': 'https://www.data.gov.in/resource/real-time-air-quality-index-various-locations',
                        'data_description': 'Only stations and pollutants with actual measurement data included'
                    },
                    'air_quality_data': self.aggregated_data
                }, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Clean data saved to {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Error saving to JSON: {e}")
            return None
    
    def get_data_summary(self) -> Dict:
        """Get summary statistics of the fetched data"""
        summary = {
            'raw_records': len(self.raw_data),
            'unique_stations': len(self.aggregated_data),
            'pollutants_found': set(),
            'stations_list': [],
            'pollutant_coverage': {}
        }
        
        if self.raw_data:
            df_raw = pd.DataFrame(self.raw_data)
            summary['date_range'] = {
                'earliest': df_raw['fetch_date'].min() if 'fetch_date' in df_raw.columns else '',
                'latest': df_raw['fetch_date'].max() if 'fetch_date' in df_raw.columns else ''
            }
            
            # Get unique pollutants
            if 'pollutant_id' in df_raw.columns:
                summary['pollutants_found'] = set(df_raw['pollutant_id'].dropna().unique())
        
        if self.aggregated_data:
            for station in self.aggregated_data:
                summary['stations_list'].append({
                    'name': station['station_name'],
                    'lat': station['latitude'],
                    'lon': station['longitude'],
                    'pollutant_count': station['total_pollutants_monitored']
                })
        
        # Count pollutant coverage
        for pollutant in summary['pollutants_found']:
            count = sum(1 for record in self.raw_data if record.get('pollutant_id') == pollutant)
            summary['pollutant_coverage'][pollutant] = count
        
        return summary

def main():
    """Main execution function"""
    # Replace with your actual API key
    API_KEY = "Your_Api_Key"
    
    if API_KEY == "your_api":
        print("⚠️  Please replace 'your_api' with your actual API key from data.gov.in")
        print("You can get your API key from: https://www.data.gov.in/help/how-use-datasets-apis")
        return
    
    # Initialize the fetcher
    fetcher = DelhiAirQualityFetcher(API_KEY)
    
    try:
        # Fetch data (adjust max_records as needed)
        print("🔄 Fetching Delhi air quality data...")
        print("📝 Note: Each API record contains data for one pollutant per station")
        data = fetcher.fetch_all_data(max_records=5000)
        
        if not data:
            print("❌ No data fetched. Please check your API key and internet connection.")
            return
        
        # Save data
        print("💾 Saving clean data...")
        csv_file = fetcher.save_to_csv()
        json_file = fetcher.save_to_json()
        
        # Print summary
        summary = fetcher.get_data_summary()
        print("\n📊 Clean Data Summary:")
        print(f"Stations with pollutant data: {summary['unique_stations']}")
        print(f"Pollutants found: {', '.join(sorted(summary['pollutants_found']))}")
        
        print(f"\n🏭 Top Stations:")
        for station in summary['stations_list'][:5]:  # Show top 5 stations
            print(f"  • {station['name']} ({station.get('pollutant_count', 0)} pollutants)")
        
        print(f"\n🧪 Data Coverage by Pollutant:")
        for pollutant, count in summary['pollutant_coverage'].items():
            print(f"  • {pollutant}: {count} station measurements")
        
        if csv_file:
            print(f"\n✅ Clean CSV saved: {csv_file}")
        if json_file:
            print(f"✅ Clean JSON saved: {json_file}")
        
        print("\n🎉 Clean data extraction completed!")
        print("\n📋 What you get:")
        print("• Only stations with actual pollutant measurements")
        print("• Only PM2.5, PM10, O3, NO2, SO2, CO, NH3 data")
        print("• No empty columns or missing data fields")
        print("• Min, max, avg values with units for each pollutant")
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    main()