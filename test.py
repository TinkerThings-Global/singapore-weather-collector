#!/usr/bin/env python3
"""
Test script to continuously paginate through API until all data is collected
"""

import httpx
import json
from datetime import date
from pprint import pprint

def test_continuous_pagination():
    """Test continuous pagination until all data is collected"""
    today = date.today()
    
    # Test temperature API
    temp_url = "https://api-open.data.gov.sg/v2/real-time/api/air-temperature"
    params = {"date": today.isoformat()}
    
    print(f"Testing continuous pagination with date: {today}")
    print(f"URL: {temp_url}")
    print("=" * 80)
    
    all_readings = []
    page_count = 0
    has_next_page = True
    
    try:
        with httpx.Client() as client:
            while has_next_page and page_count < 100:  # Safety limit
                page_count += 1
                print(f"\n{'='*20} PAGE {page_count} {'='*20}")
                print(f"Request params: {params}")
                
                response = client.get(temp_url, params=params, timeout=30)
                print(f"Status Code: {response.status_code}")
                print(f"Response URL: {response.url}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract readings from this page
                    if "data" in data and "readings" in data["data"]:
                        readings = data["data"]["readings"]
                        all_readings.extend(readings)
                        
                        print(f"Readings in this page: {len(readings)}")
                        print(f"Total readings collected: {len(all_readings)}")
                        
                        if readings:
                            print(f"First timestamp: {readings[0]['timestamp']}")
                            print(f"Last timestamp: {readings[-1]['timestamp']}")
                            
                            # Check for S50 station data
                            s50_data = []
                            for reading in readings:
                                for station_data in reading.get('data', []):
                                    if station_data.get('stationId') == 'S50':
                                        s50_data.append({
                                            'timestamp': reading['timestamp'],
                                            'value': station_data['value']
                                        })
                            print(f"S50 readings in this page: {len(s50_data)}")
                        
                        # Check for next page
                        if "paginationToken" in data["data"]:
                            pagination_token = data["data"]["paginationToken"]
                            print(f"Pagination Token: {pagination_token}")
                            params["paginationToken"] = pagination_token
                        else:
                            has_next_page = False
                            print("No pagination token - reached end")
                    else:
                        print("No readings found in response")
                        has_next_page = False
                        
                    # Print condensed response structure
                    print("Response structure:")
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if key == "data" and isinstance(value, dict):
                                data_keys = list(value.keys())
                                print(f"  {key}: dict with keys {data_keys}")
                                if "readings" in value:
                                    print(f"    readings: {len(value['readings'])} items")
                                if "paginationToken" in value:
                                    print(f"    paginationToken: {value['paginationToken']}")
                            else:
                                print(f"  {key}: {type(value).__name__} = {value}")
                                
                else:
                    print(f"Error: HTTP {response.status_code}")
                    print("Response text:")
                    print(response.text)
                    break
                    
                # Small delay to avoid hitting rate limits
                import time
                time.sleep(0.1)
                
        print(f"\n{'='*60}")
        print("FINAL SUMMARY:")
        print(f"Total pages fetched: {page_count}")
        print(f"Total readings collected: {len(all_readings)}")
        
        # Analyze S50 data across all pages
        all_s50_data = []
        for reading in all_readings:
            for station_data in reading.get('data', []):
                if station_data.get('stationId') == 'S50':
                    all_s50_data.append({
                        'timestamp': reading['timestamp'],
                        'value': station_data['value']
                    })
                    
        print(f"Total S50 readings: {len(all_s50_data)}")
        if all_s50_data:
            print(f"S50 time range: {all_s50_data[0]['timestamp']} to {all_s50_data[-1]['timestamp']}")
            
        if len(all_readings) >= 1440:
            print("✅ Collected full day of data (1440+ readings)")
        else:
            print(f"⚠️  Only collected {len(all_readings)} readings (expected ~1440 for full day)")
                
    except Exception as e:
        print(f"Error during pagination: {e}")

if __name__ == "__main__":
    test_continuous_pagination()