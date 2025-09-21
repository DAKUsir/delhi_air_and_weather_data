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
    def __init__(self, api_key: str, openweather_api_key: str = None):
        self.api_key = api_key
        self.openweather_api_key = openweather_api_key
        self.base_url = "https://api.data.gov.in/resource"
        self.resource_id = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"  # Air Quality Index resource ID
        self.openweather_base_url = "https://api.openweathermap.org/data/2.5"
        self.headers = {
            'User-Agent': 'Delhi-Air-Quality-Fetcher/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        self.raw_data = []
        self.aggregated_data = []
        self.weather_data = {}
        
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
    
    def fetch_weather_data(self, lat: float, lon: float, station_name: str = "") -> Dict:
        """Fetch current weather data for given coordinates"""
        if not self.openweather_api_key:
            return {}
            
        try:
            # Current weather
            current_url = f"{self.openweather_base_url}/weather"
            current_params = {
                'lat': lat,
                'lon': lon,
                'appid': self.openweather_api_key,
                'units': 'metric'  # Celsius, m/s, etc.
            }
            
            logging.info(f"Fetching weather for {station_name} ({lat}, {lon})")
            current_response = requests.get(current_url, params=current_params, timeout=30)
            current_response.raise_for_status()
            current_data = current_response.json()
            
            # Air pollution data from OpenWeather (if available)
            pollution_data = {}
            try:
                pollution_url = f"{self.openweather_base_url}/air_pollution"
                pollution_params = {
                    'lat': lat,
                    'lon': lon,
                    'appid': self.openweather_api_key
                }
                pollution_response = requests.get(pollution_url, params=pollution_params, timeout=30)
                if pollution_response.status_code == 200:
                    pollution_json = pollution_response.json()
                    if 'list' in pollution_json and pollution_json['list']:
                        pollution_data = pollution_json['list'][0]
            except Exception as e:
                logging.warning(f"Could not fetch pollution data from OpenWeather: {e}")
            
            # Extract relevant weather data
            weather_info = {
                'weather_timestamp': datetime.now().isoformat(),
                'weather_main': current_data.get('weather', [{}])[0].get('main', ''),
                'weather_description': current_data.get('weather', [{}])[0].get('description', ''),
                'temperature': current_data.get('main', {}).get('temp'),
                'feels_like': current_data.get('main', {}).get('feels_like'),
                'humidity': current_data.get('main', {}).get('humidity'),
                'pressure': current_data.get('main', {}).get('pressure'),
                'visibility': current_data.get('visibility'),
                'wind_speed': current_data.get('wind', {}).get('speed'),
                'wind_direction': current_data.get('wind', {}).get('deg'),
                'wind_gust': current_data.get('wind', {}).get('gust'),
                'cloudiness': current_data.get('clouds', {}).get('all'),
                'uv_index': None,  # Would need separate UV Index API call
                'sunrise': current_data.get('sys', {}).get('sunrise'),
                'sunset': current_data.get('sys', {}).get('sunset')
            }
            
            # Add OpenWeather AQI if available
            if pollution_data:
                weather_info['openweather_aqi'] = pollution_data.get('main', {}).get('aqi')
                components = pollution_data.get('components', {})
                weather_info['ow_co'] = components.get('co')
                weather_info['ow_no'] = components.get('no')
                weather_info['ow_no2'] = components.get('no2')
                weather_info['ow_o3'] = components.get('o3')
                weather_info['ow_so2'] = components.get('so2')
                weather_info['ow_pm25'] = components.get('pm2_5')
                weather_info['ow_pm10'] = components.get('pm10')
                weather_info['ow_nh3'] = components.get('nh3')
            
            return weather_info
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Weather API error for {station_name}: {e}")
            return {}
        except Exception as e:
            logging.error(f"Weather processing error for {station_name}: {e}")
            return {}
    
    def get_delhi_general_weather(self) -> Dict:
        """Get general weather for Delhi city center"""
        delhi_lat, delhi_lon = 28.6139, 77.2090  # Delhi coordinates
        return self.fetch_weather_data(delhi_lat, delhi_lon, "Delhi Center")
    
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
            
            # Add weather data if available
            if self.openweather_api_key and station_info['latitude'] and station_info['longitude']:
                weather_data = self.fetch_weather_data(
                    station_info['latitude'], 
                    station_info['longitude'], 
                    station_info['station_name']
                )
                clean_record.update(weather_data)
                time.sleep(0.5)  # Rate limiting for weather API
            
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
        if self.openweather_api_key:
            logging.info("Weather data will be added for each station...")
        self.aggregated_data = self.aggregate_by_station(self.raw_data)
        
        # Add general Delhi weather data
        if self.openweather_api_key:
            logging.info("Fetching general Delhi weather...")
            self.weather_data = self.get_delhi_general_weather()
        
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
            filename = f"delhi_air_weather_quality_clean_{timestamp}.csv"
        
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
            filename = f"delhi_air_weather_quality_clean_{timestamp}.json"
        
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
                        'weather_data_included': bool(self.openweather_api_key),
                        'source': 'https://www.data.gov.in/resource/real-time-air-quality-index-various-locations',
                        'weather_source': 'OpenWeatherMap API' if self.openweather_api_key else None,
                        'data_description': 'Only stations and pollutants with actual measurement data included'
                    },
                    'delhi_general_weather': self.weather_data,
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
    # Replace with your actual API keys
    API_KEY = "your_api"
    OPENWEATHER_API_KEY = "your_openweather_api"  
    
    if API_KEY == "your_api":
        print("⚠️  Please replace 'your_api' with your actual API key from data.gov.in")
        print("You can get your API key from: https://www.data.gov.in/help/how-use-datasets-apis")
        return
    
    if OPENWEATHER_API_KEY == "your_openweather_api":
        print("⚠️  Please replace 'your_openweather_api' with your OpenWeather API key")
        print("You can get your API key from: https://openweathermap.org/api")
        print("Note: Script will work without weather data if you don't have OpenWeather API key")
        OPENWEATHER_API_KEY = None
    
    # Initialize the fetcher
    fetcher = DelhiAirQualityFetcher(API_KEY, OPENWEATHER_API_KEY)
    
    try:
        # Fetch data (adjust max_records as needed)
        print("🔄 Fetching Delhi air quality data...")
        if OPENWEATHER_API_KEY:
            print("🌤️  Weather data will be included for each station")
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
        if OPENWEATHER_API_KEY:
            print("🌤️  Weather data included for each station")
        
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
        if OPENWEATHER_API_KEY:
            print("• Current weather data for each station location")
            print("• Temperature, humidity, pressure, wind, visibility")
            print("• Weather conditions and cloudiness")
            print("• Optional: OpenWeather air quality data for comparison")
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    main()