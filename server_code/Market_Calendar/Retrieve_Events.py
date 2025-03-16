import anvil.server
import anvil.http
from bs4 import BeautifulSoup
import datetime
import re
import pytz
import json
from ..Shared_Functions import DB_Utils

# Constants
FOREXFACTORY_BASE_URL = "https://www.forexfactory.com/calendar"
USD_CURRENCY = "USD"
DEFAULT_TIMEZONE = "America/New_York"  # Default to Eastern time if we can't detect

# Helper Functions
def _get_response_text(url):
    """Helper function to fetch and process HTTP responses"""
    print(f"Sending HTTP request to {url}")
    response = anvil.http.request(url, json=False)
    
    if not response:
        print("Failed to retrieve the calendar page")
        return None
    
    print("Successfully retrieved calendar page")
    
    # Convert the response to a string if it's a streaming object
    try:
        return response.get_bytes().decode('utf-8')
    except AttributeError:
        # If it's already a string, use it as is
        return response

def _detect_site_timezone(response_text):
    """Extract the timezone information from the site response"""
    # Look for timezone information in the response
    timezone_pattern = r'timezone:\s*[\'"]([^\'"]+)[\'"]'
    timezone_match = re.search(timezone_pattern, response_text)
    
    if timezone_match:
        detected_timezone = timezone_match.group(1)
        print(f"Extracted site timezone: {detected_timezone}")
        try:
            # Verify it's a valid timezone
            pytz.timezone(detected_timezone)
            return detected_timezone
        except pytz.exceptions.UnknownTimeZoneError:
            print(f"Unknown timezone: {detected_timezone}, falling back to default")
    else:
        print("No timezone information found in the response")
    
    # If no timezone found or it's invalid, use default
    return DEFAULT_TIMEZONE

def _convert_to_utc(dt, source_timezone):
    """Convert a datetime from source timezone to UTC"""
    if not dt:
        return dt
    
    # Make the datetime timezone-aware in the source timezone
    source_tz = pytz.timezone(source_timezone)
    aware_dt = source_tz.localize(dt)
    
    # Convert to UTC
    utc_dt = aware_dt.astimezone(pytz.UTC)
    print(f"Converted {dt} ({source_timezone}) to UTC: {utc_dt}")
    
    return utc_dt

def _extract_calendar_events(response_text):
    """
    Extract calendar events from the ForexFactory HTML response by parsing the embedded JavaScript data
    
    The ForexFactory calendar embeds all event data in a JavaScript object:
    window.calendarComponentStates[1] = {
        days: [{
            "date": "Mon <span>Mar 17</span>",
            "dateline": 1742169600,
            "events": [{
                "id": 142347,
                "name": "Core Retail Sales m/m",
                "country": "US",
                "currency": "USD",
                "impactClass": "icon--ff-impact-red",
                "impactTitle": "High Impact Expected",
                "timeLabel": "12:30pm",
                "previous": "-0.4%",
                "forecast": "0.3%"
                ...
            }]
        }]
    }
    """
    events = []
    
    # Detect the site's timezone
    source_timezone = _detect_site_timezone(response_text)
    
    try:
        print("\nExtracting calendar events from JavaScript data")
        
        # Find the JavaScript data object in the HTML
        calendar_data_pattern = r'window\.calendarComponentStates\[\d+\]\s*=\s*(\{.*?days:\s*\[.*?\]\s*,\s*time:.*?\});'
        calendar_data_match = re.search(calendar_data_pattern, response_text, re.DOTALL)
        
        if not calendar_data_match:
            print("Could not find calendar data in the response")
            return _extract_events_with_regex(response_text, source_timezone)
        
        print("Found calendar data in JavaScript")
        
        # Extract the days array which contains all events
        days_pattern = r'days:\s*(\[.*?\])'
        days_match = re.search(days_pattern, calendar_data_match.group(1), re.DOTALL)
        
        if not days_match:
            print("Could not find days array in calendar data")
            return _extract_events_with_regex(response_text, source_timezone)
        
        print("Found days array in calendar data")
        
        # Parse the JavaScript object (clean it up for Python)
        days_json = days_match.group(1)
        
        # Handle problematic JSON content in multiple steps
        # Step 1: Add quotes to keys
        days_json = re.sub(r'([{,])\s*(\w+):', r'\1"\2":', days_json)
        
        # Step 2: Fix escaped slashes
        days_json = days_json.replace('\\/', '/')
        
        # Step 3: Fix any unquoted values, carefully avoiding arrays and objects
        days_json = re.sub(r':\s*([^",\s\[\{][^",\]\}]*?)([,\}\]])', r':"\1"\2', days_json)
        
        # Step 4: Fix boolean values to be proper JSON
        days_json = re.sub(r':(\s*)(true|false)([,\}\]])', r':\1\2\3', days_json)
        
        # Step 5: Handle the "firstInDay:true" pattern specifically
        days_json = re.sub(r'"firstInDay":true', r'"firstInDay":true', days_json)
        days_json = re.sub(r'"firstInDay":false', r'"firstInDay":false', days_json)
        
        # Step 6: Try to fix any other issues that might cause JSON parsing to fail
        # Remove any trailing commas in arrays or objects
        days_json = re.sub(r',(\s*[\]\}])', r'\1', days_json)
        
        print(f"Processing JSON data...")
        
        try:
            days_data = json.loads(days_json)
            print("Successfully parsed days JSON data")
        except json.JSONDecodeError as e:
            print(f"Error parsing days JSON: {e}")
            # Fall back to regex approach
            return _extract_events_with_regex(response_text, source_timezone)
        
        # Process each day's events
        for day_data in days_data:
            date_text = re.sub(r'<[^>]+>', '', day_data.get('date', ''))  # Remove HTML tags
            date_text = date_text.strip()
            day_date = None
            
            # Try to parse the date
            try:
                # Convert to a date object
                day_date = datetime.datetime.fromtimestamp(int(day_data.get('dateline', 0)))
                print(f"Processing date: {day_date.strftime('%Y-%m-%d')}")
            except (ValueError, TypeError):
                print(f"Could not parse date from: {date_text}")
                continue
            
            # Process events for this day
            day_events = day_data.get('events', [])
            print(f"Found {len(day_events)} events for this day")
            
            for event_data in day_events:
                try:
                    # Extract basic event information
                    event_name = event_data.get('name', '')
                    country = event_data.get('country', '')
                    currency = event_data.get('currency', '')
                    time_label = event_data.get('timeLabel', '')
                    
                    # Skip non-USD events if configured to do so
                    if currency != USD_CURRENCY:
                        continue
                    
                    # Convert impact class to our standard format
                    impact_class = event_data.get('impactClass', '')
                    impact_title = event_data.get('impactTitle', '')
                    impact = _map_impact_level(impact_class, impact_title)
                    
                    # Get forecast and previous values
                    forecast = event_data.get('forecast', '')
                    previous = event_data.get('previous', '')
                    
                    # Create event datetime (combining date with time)
                    event_datetime = day_date
                    if time_label:
                        # Parse the time (e.g., "12:30pm") and add it to the date
                        try:
                            # ForexFactory uses 12-hour format with am/pm
                            time_parts = re.match(r'(\d+):(\d+)(am|pm)', time_label.lower())
                            if time_parts:
                                hour = int(time_parts.group(1))
                                minute = int(time_parts.group(2))
                                am_pm = time_parts.group(3)
                                
                                # Convert to 24-hour format
                                if am_pm == 'pm' and hour < 12:
                                    hour += 12
                                elif am_pm == 'am' and hour == 12:
                                    hour = 0
                                
                                # Update the event datetime
                                event_datetime = event_datetime.replace(hour=hour, minute=minute)
                        except Exception as e:
                            print(f"Error parsing time '{time_label}' for event '{event_name}': {e}")
                    
                    # Convert the event datetime to UTC
                    utc_event_datetime = _convert_to_utc(event_datetime, source_timezone)
                    
                    # Build the event object - Use 'event' as the key instead of 'name' for compatibility
                    event = {
                        'date': utc_event_datetime.strftime('%Y-%m-%d'),
                        'time': utc_event_datetime.strftime('%H:%M'),
                        'currency': currency,
                        'event': event_name,  # Changed from 'name' to 'event' for compatibility
                        'impact': impact,
                        'forecast': forecast,
                        'previous': previous,
                        'source': 'ForexFactory',
                        'timezone': 'UTC'  # Store timezone information
                    }
                    
                    print(f"Extracted event: {event_name} at {time_label} with impact {impact}")
                    events.append(event)
                    
                except Exception as e:
                    print(f"Error processing event: {e}")
                    continue
        
    except Exception as e:
        print(f"Exception in calendar extraction: {e}")
        # Fall back to regex approach
        return _extract_events_with_regex(response_text, source_timezone)
    
    print(f"Extracted {len(events)} total events")
    return events

def _extract_events_with_regex(response_text, source_timezone=DEFAULT_TIMEZONE):
    """Fallback method to extract events using regex if JSON parsing fails"""
    print("Using regex fallback method to extract events")
    events = []
    
    # Look for individual event objects in the JavaScript
    event_pattern = r'"id":\s*(\d+).*?"name":\s*"([^"]+)".*?"country":\s*"([^"]+)".*?"currency":\s*"([^"]+)".*?"impactClass":\s*"([^"]+)".*?"timeLabel":\s*"([^"]+)".*?"previous":\s*"([^"]*)".*?"forecast":\s*"([^"]*)".*?"date":\s*"([^"]+)"'
    
    event_matches = list(re.finditer(event_pattern, response_text, re.DOTALL))
    print(f"Found {len(event_matches)} event matches using regex")
    
    for match in event_matches:
        try:
            event_id, name, country, currency, impact_class, time_label, previous, forecast, date_str = match.groups()
            
            # Skip non-USD events if configured to do so
            if currency != USD_CURRENCY:
                continue
            
            # Parse the date string
            try:
                date_parts = date_str.split(', ')
                if len(date_parts) == 2:
                    date_obj = datetime.datetime.strptime(date_parts[1], '%Y')  # Just get the year
                    month_day = date_parts[0].split(' ')
                    if len(month_day) == 2:
                        month = month_day[0]
                        day = month_day[1]
                        # Convert month name to number
                        month_num = {
                            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                        }.get(month, 1)
                        
                        date_obj = date_obj.replace(month=month_num, day=int(day))
                else:
                    # If date parsing fails, use today's date as a fallback
                    date_obj = datetime.datetime.now()
            except Exception:
                date_obj = datetime.datetime.now()
            
            # Map impact level
            impact = _map_impact_level(impact_class, "")
            
            # Try to parse time label and create full datetime
            event_datetime = date_obj
            if re.match(r'(\d+):(\d+)(am|pm)', time_label.lower()):
                try:
                    # Parse 12-hour format
                    time_parts = re.match(r'(\d+):(\d+)(am|pm)', time_label.lower())
                    if time_parts:
                        hour = int(time_parts.group(1))
                        minute = int(time_parts.group(2))
                        am_pm = time_parts.group(3)
                        
                        # Convert to 24-hour format
                        if am_pm == 'pm' and hour < 12:
                            hour += 12
                        elif am_pm == 'am' and hour == 12:
                            hour = 0
                        
                        # Update the event datetime
                        event_datetime = event_datetime.replace(hour=hour, minute=minute)
                except:
                    pass
            
            # Convert to UTC
            utc_event_datetime = _convert_to_utc(event_datetime, source_timezone)
            event_time = utc_event_datetime.strftime('%H:%M')
            
            # Create the event - Use 'event' as the key instead of 'name' for compatibility
            event = {
                'date': utc_event_datetime.strftime('%Y-%m-%d'),
                'time': event_time,
                'currency': currency,
                'event': name,  # Changed from 'name' to 'event' for compatibility 
                'impact': impact,
                'forecast': forecast,
                'previous': previous,
                'source': 'ForexFactory',
                'timezone': 'UTC'  # Store timezone information
            }
            
            print(f"Extracted event via regex: {name} at {time_label} with impact {impact}")
            events.append(event)
        except Exception as e:
            print(f"Error processing regex match: {e}")
    
    print(f"Extracted {len(events)} total events via regex")
    return events

def _map_impact_level(impact_class, impact_title):
    """Map ForexFactory impact class/title to our standard impact levels"""
    if 'ff-impact-red' in impact_class or 'High Impact' in impact_title:
        return 'High'
    elif 'ff-impact-ora' in impact_class or 'Medium Impact' in impact_title:
        return 'Medium'
    elif 'ff-impact-yel' in impact_class or 'Low Impact' in impact_title:
        return 'Low'
    else:
        return ''

def _fetch_and_save_events(url):
    """
    Fetch events from a given ForexFactory URL and save them to the database
    
    Args:
        url: Complete ForexFactory URL to fetch events from
        
    Returns:
        dict: Statistics about processed events
    """
    # Get the HTML response
    response_text = _get_response_text(url)
    if not response_text:
        print(f"Failed to get response from {url}")
        return {"total": 0, "existing": 0, "new": 0}
    
    # Extract events from the HTML
    events = _extract_calendar_events(response_text)
    if not events:
        print("No events extracted from the page")
        return {"total": 0, "existing": 0, "new": 0}
    
    # Save events to the database
    stats = DB_Utils.save_multiple_market_calendar_events(events)
    
    # Format the statistics output
    print("\nEvent Processing Summary:")
    print(f"Total Scraped Events: {stats['total']}")
    print(f"Skipped (existing) events: {stats['existing']}")
    print(f"Newly added events: {stats['new']}")
    print()
    
    return stats

@anvil.server.callable
def fetch_tomorrow_events():
    """
    Fetch and save market calendar events for tomorrow from ForexFactory
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?day=tomorrow"
    print(f"Fetching tomorrow's events from: {url}")
    
    return _fetch_and_save_events(url)

@anvil.server.callable
def fetch_this_week_events():
    """
    Fetch and save market calendar events for the current week from ForexFactory
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?week=this"
    print(f"Fetching this week's events from: {url}")
    
    return _fetch_and_save_events(url)

@anvil.server.callable
def fetch_next_week_events():
    """
    Fetch and save market calendar events for next week from ForexFactory
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?week=next"
    print(f"Fetching next week's events from: {url}")
    
    return _fetch_and_save_events(url)

@anvil.server.callable
def fetch_this_month_events():
    """
    Fetch and save market calendar events for the current month from ForexFactory
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?month=this"
    print(f"Fetching this month's events from: {url}")
    
    return _fetch_and_save_events(url)

@anvil.server.callable
def fetch_next_month_events():
    """
    Fetch and save market calendar events for next month from ForexFactory
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?month=next"
    print(f"Fetching next month's events from: {url}")
    
    return _fetch_and_save_events(url)

@anvil.server.callable
@anvil.server.background_task
def refresh_all_calendars():
    """
    Refresh all calendar periods (tomorrow, this week, next week, this month, next month)
    
    Returns:
        dict: Combined statistics for all time ranges
    """
    # Initialize combined statistics
    combined_stats = {
        "total": 0,
        "existing": 0,
        "new": 0,
        "details": {}  # Store individual statistics for each period
    }
    
    # Fetch and process events for each time period
    time_ranges = {
        "tomorrow": fetch_tomorrow_events,
        "this_week": fetch_this_week_events,
        "next_week": fetch_next_week_events,
        "this_month": fetch_this_month_events,
        "next_month": fetch_next_month_events
    }
    
    for period_name, fetch_function in time_ranges.items():
        print(f"\nProcessing {period_name} calendar...")
        stats = fetch_function()
        
        # Add to combined totals
        combined_stats["total"] += stats["total"]
        combined_stats["existing"] += stats["existing"]
        combined_stats["new"] += stats["new"]
        
        # Store individual statistics
        combined_stats["details"][period_name] = stats
    
    # Print combined statistics
    print("\n=== COMBINED EVENT PROCESSING SUMMARY ===")
    print(f"Total Scraped Events: {combined_stats['total']}")
    print(f"Skipped (existing) events: {combined_stats['existing']}")
    print(f"Newly added events: {combined_stats['new']}")
    print("=======================================\n")
    
    return combined_stats

@anvil.server.callable
def retrieve_market_calendar_events():
    """Legacy function - redirects to refresh_all_calendars"""
    print("Legacy function retrieve_market_calendar_events called - using refresh_all_calendars instead")
    return refresh_all_calendars()

@anvil.server.callable
def retrieve_market_calendar_events_this_month():
    """Legacy function - redirects to fetch_this_month_events"""
    print("Legacy function retrieve_market_calendar_events_this_month called - using fetch_this_month_events instead")
    return fetch_this_month_events()

@anvil.server.callable
def retrieve_market_calendar_events_next_month():
    """Legacy function - redirects to fetch_next_month_events"""
    print("Legacy function retrieve_market_calendar_events_next_month called - using fetch_next_month_events instead")
    return fetch_next_month_events()

# You can test these functions using the uplink with:
# anvil.server.call('fetch_tomorrow_events')
# anvil.server.call('fetch_this_week_events')
# anvil.server.call('fetch_next_week_events')
# anvil.server.call('fetch_this_month_events')
# anvil.server.call('fetch_next_month_events')
# anvil.server.call('refresh_all_calendars')
