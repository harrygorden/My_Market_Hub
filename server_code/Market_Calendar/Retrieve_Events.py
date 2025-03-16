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
VERBOSE_LOGGING = True  # Set to False to reduce logging verbosity

# Helper Functions
def _get_response_text(url, verbose=VERBOSE_LOGGING):
    """
    Send an HTTP request to retrieve the calendar page content
    
    Args:
        url (str): Calendar page URL
        verbose (bool): Whether to print detailed logs
        
    Returns:
        str: HTML response text
    """
    try:
        if verbose:
            print(f"Sending HTTP request to {url}")
        
        # Use the Anvil HTTP library to fetch the URL content
        response = anvil.http.request(url, method="GET")
        
        if verbose:
            print("Successfully retrieved calendar page")
        
        # Ensure we're returning a string/text rather than a response object
        if hasattr(response, 'get_bytes'):
            # If it's a streaming response, get the bytes and decode
            return response.get_bytes().decode('utf-8')
        elif isinstance(response, (str, bytes)):
            # If it's already a string or bytes, handle appropriately
            if isinstance(response, bytes):
                return response.decode('utf-8')
            return response
        else:
            # For other response types, convert to string
            return str(response)
    except Exception as e:
        print(f"Error fetching URL {url}: {str(e)}")
        return ""

def _detect_site_timezone(response_text, verbose=VERBOSE_LOGGING):
    """
    Detect the timezone used by the Forex Factory website
    
    Args:
        response_text (str): HTML response text
        verbose (bool): Whether to print detailed logs
        
    Returns:
        str: Timezone string (e.g., 'US/Eastern')
    """
    if not response_text:
        if verbose:
            print("No response text to detect timezone, using default")
        return DEFAULT_TIMEZONE
        
    # Try to find timezone information in the meta tags or text
    timezone_pattern = r'timezone=([^"&]+)'
    match = re.search(timezone_pattern, response_text)
    
    if match:
        timezone_value = match.group(1)
        if verbose:
            print(f"Extracted site timezone: {timezone_value}")
        
        # Check if it's a numeric timezone (e.g., "0" for UTC)
        if timezone_value.strip().lstrip('-').isdigit() or timezone_value.strip() == "0":
            if verbose:
                print(f"Numeric timezone '{timezone_value}' detected as UTC/GMT")
            return "UTC"  # Treat numeric timezones as UTC/GMT
        
        # Add common timezone mappings if needed
        timezone_mappings = {
            'est': 'US/Eastern',
            'edt': 'US/Eastern',
            'eastern': 'US/Eastern',
            'cst': 'US/Central',
            'cdt': 'US/Central',
            'central': 'US/Central',
            'mst': 'US/Mountain',
            'mdt': 'US/Mountain',
            'mountain': 'US/Mountain',
            'pst': 'US/Pacific',
            'pdt': 'US/Pacific',
            'pacific': 'US/Pacific',
            'gmt': 'UTC',
            'utc': 'UTC',
        }
        
        # Convert to lowercase for case-insensitive matching
        timezone_key = timezone_value.lower()
        
        if timezone_key in timezone_mappings:
            return timezone_mappings[timezone_key]
        
        return timezone_value  # Return as-is if no mapping found
    
    # For ForexFactory site: Look for timezone indicator in the page content
    # Sometimes the timezone is shown in text like "All times are GMT" or similar
    time_indicator_pattern = r'All times are ([A-Z]{3})'
    match = re.search(time_indicator_pattern, response_text)
    
    if match:
        timezone_abbreviation = match.group(1)
        
        # Map common abbreviations
        timezone_abbr_map = {
            'GMT': 'UTC',
            'UTC': 'UTC',
            'EST': 'US/Eastern',
            'EDT': 'US/Eastern',
            'CST': 'US/Central',
            'CDT': 'US/Central',
            'MST': 'US/Mountain',
            'MDT': 'US/Mountain',
            'PST': 'US/Pacific',
            'PDT': 'US/Pacific',
        }
        
        if timezone_abbreviation in timezone_abbr_map:
            if verbose:
                print(f"Found timezone indicator: {timezone_abbreviation}")
            return timezone_abbr_map[timezone_abbreviation]
    
    # If we couldn't detect the timezone, default to Eastern Time
    # This is common for US market calendar sites
    if verbose:
        print("Could not detect timezone, using default (Eastern)")
    return DEFAULT_TIMEZONE

def _convert_to_utc(dt, source_timezone, verbose=VERBOSE_LOGGING):
    """
    Convert a datetime from source timezone to UTC
    
    Args:
        dt: The datetime to convert
        source_timezone: Source timezone identifier
        verbose: Whether to print detailed logs
        
    Returns:
        datetime.datetime: UTC datetime
    """
    if not dt:
        return dt
    
    original_dt = dt
    
    try:
        # Make the datetime timezone-aware in the source timezone
        source_tz = pytz.timezone(source_timezone)
        aware_dt = source_tz.localize(dt)
        
        # Convert to UTC
        utc_dt = aware_dt.astimezone(pytz.UTC)
        
        # If we're getting times that are 4 hours ahead of what they should be,
        # apply a correction by subtracting 4 hours
        utc_dt = utc_dt - datetime.timedelta(hours=4)
        
        if verbose:
            print(f"Time conversion details:")
            print(f"  Original datetime: {original_dt}")
            print(f"  Source timezone: {source_timezone}")
            print(f"  After localization: {aware_dt}")
            print(f"  After UTC conversion: {aware_dt.astimezone(pytz.UTC)}")
            print(f"  After 4-hour correction: {utc_dt}")
        
        return utc_dt
    except Exception as e:
        if verbose:
            print(f"Error in timezone conversion: {e}")
            print(f"Original datetime: {original_dt}, Source timezone: {source_timezone}")
        # Return the original datetime if conversion fails
        return dt

def _extract_events_from_javascript(response_text, source_timezone=DEFAULT_TIMEZONE, verbose=VERBOSE_LOGGING):
    """
    Extract events from the JavaScript data in the ForexFactory calendar page
    
    Args:
        response_text (str): HTML response text
        source_timezone (str): Timezone of the calendar page
        verbose (bool): Whether to print detailed logs
        
    Returns:
        list: List of event dictionaries
    """
    events = []
    
    try:
        # Find the calendar data in the JavaScript
        calendar_data_pattern = r'var\s+calendarJSON\s*=\s*({[^;]+});'
        match = re.search(calendar_data_pattern, response_text)
        
        if not match:
            if verbose:
                print("Could not find calendar data in JavaScript")
            return _extract_events_with_regex(response_text, source_timezone, verbose)
        
        if verbose:
            print("Found calendar data in JavaScript")
        
        # Extract the calendar JSON data
        calendar_json = match.group(1)
        
        # Extract the days array from the calendar data
        days_pattern = r'"days"\s*:\s*(\[[^]]*\])'
        days_match = re.search(days_pattern, calendar_json)
        
        if not days_match:
            if verbose:
                print("Could not find days array in calendar data")
            return _extract_events_with_regex(response_text, source_timezone, verbose)
        
        if verbose:
            print("Found days array in calendar data")
        
        days_json = days_match.group(1)
        
        if verbose:
            print(f"Processing JSON data...")
        
        try:
            days_data = json.loads(days_json)
            if verbose:
                print("Successfully parsed days JSON data")
        except json.JSONDecodeError as e:
            if verbose:
                print(f"Error parsing days JSON: {e}")
            # Fall back to regex approach
            return _extract_events_with_regex(response_text, source_timezone, verbose)
        
        # Process each day's events
        for day_data in days_data:
            date_text = re.sub(r'<[^>]+>', '', day_data.get('date', ''))  # Remove HTML tags
            date_text = date_text.strip()
            day_date = None
            
            # Try to parse the date
            try:
                # Convert to a date object
                day_date = datetime.datetime.fromtimestamp(int(day_data.get('dateline', 0)))
                if verbose:
                    print(f"Processing date: {day_date.strftime('%Y-%m-%d')}")
            except (ValueError, TypeError):
                if verbose:
                    print(f"Could not parse date from: {date_text}")
                continue
            
            # Process events for this day
            day_events = day_data.get('events', [])
            if verbose:
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
                            if verbose:
                                print(f"Error parsing time '{time_label}' for event '{event_name}': {e}")
                    
                    # Convert the event datetime to UTC
                    utc_event_datetime = _convert_to_utc(event_datetime, source_timezone, verbose)
                    
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
                    
                    if verbose:
                        print(f"Extracted event: {event_name} at {time_label} with impact {impact}")
                    events.append(event)
                    
                except Exception as e:
                    if verbose:
                        print(f"Error processing event: {e}")
                    continue
        
    except Exception as e:
        if verbose:
            print(f"Exception in calendar extraction: {e}")
        # Fall back to regex approach
        return _extract_events_with_regex(response_text, source_timezone, verbose)
    
    if verbose:
        print(f"Extracted {len(events)} total events")
    return events

def _extract_events_with_regex(response_text, source_timezone=DEFAULT_TIMEZONE, verbose=VERBOSE_LOGGING):
    """
    Fallback method to extract events using regex if JSON parsing fails
    
    Args:
        response_text (str): HTML response text
        source_timezone (str): Timezone of the calendar page
        verbose (bool): Whether to print detailed logs
        
    Returns:
        list: List of event dictionaries
    """
    if verbose:
        print("Using regex fallback method to extract events")
    events = []
    
    # Look for individual event objects in the JavaScript
    event_pattern = r'"id":\s*(\d+).*?"name":\s*"([^"]+)".*?"country":\s*"([^"]+)".*?"currency":\s*"([^"]+)".*?"impactClass":\s*"([^"]+)".*?"timeLabel":\s*"([^"]+)".*?"previous":\s*"([^"]*)".*?"forecast":\s*"([^"]*)".*?"date":\s*"([^"]+)"'
    
    event_matches = list(re.finditer(event_pattern, response_text, re.DOTALL))
    if verbose:
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
            utc_event_datetime = _convert_to_utc(event_datetime, source_timezone, verbose)
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
            
            if verbose:
                print(f"Extracted event via regex: {name} at {time_label} with impact {impact}")
            events.append(event)
        except Exception as e:
            if verbose:
                print(f"Error processing regex match: {e}")
    
    if verbose:
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

def _fetch_and_save_events(url, verbose=VERBOSE_LOGGING):
    """
    Fetch events from a given ForexFactory URL and save them to the database
    
    Args:
        url: Complete ForexFactory URL to fetch events from
        verbose: Whether to print detailed logs
        
    Returns:
        dict: Statistics about processed events
    """
    # Get the HTML response
    response_text = _get_response_text(url, verbose)
    if not response_text:
        print(f"Failed to get response from {url}")
        return {"total": 0, "existing": 0, "new": 0}
    
    # Extract events from the HTML
    events = _extract_events_from_javascript(response_text, verbose=verbose)
    if not events:
        print("No events extracted from the page")
        return {"total": 0, "existing": 0, "new": 0}
    
    # Save events to the database
    stats = DB_Utils.save_multiple_market_calendar_events(events, verbose)
    
    # Format the statistics output
    if verbose:
        print("\nEvent Processing Summary:")
        print(f"Total Scraped Events: {stats['total']}")
        print(f"Skipped (existing) events: {stats['existing']}")
        print(f"Newly added events: {stats['new']}")
        print()
    
    return stats

@anvil.server.callable
def fetch_tomorrow_events(verbose=VERBOSE_LOGGING):
    """
    Fetch and save market calendar events for tomorrow from ForexFactory
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?day=tomorrow"
    if verbose:
        print(f"Fetching tomorrow's events from: {url}")
    
    return _fetch_and_save_events(url, verbose)

@anvil.server.callable
def fetch_this_week_events(verbose=VERBOSE_LOGGING):
    """
    Fetch and save market calendar events for the current week from ForexFactory
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?week=this"
    if verbose:
        print(f"Fetching this week's events from: {url}")
    
    return _fetch_and_save_events(url, verbose)

@anvil.server.callable
def fetch_next_week_events(verbose=VERBOSE_LOGGING):
    """
    Fetch and save market calendar events for next week from ForexFactory
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?week=next"
    if verbose:
        print(f"Fetching next week's events from: {url}")
    
    return _fetch_and_save_events(url, verbose)

@anvil.server.callable
def fetch_this_month_events(verbose=VERBOSE_LOGGING):
    """
    Fetch and save market calendar events for the current month from ForexFactory
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?month=this"
    if verbose:
        print(f"Fetching this month's events from: {url}")
    
    return _fetch_and_save_events(url, verbose)

@anvil.server.callable
def fetch_next_month_events(verbose=VERBOSE_LOGGING):
    """
    Fetch and save market calendar events for next month from ForexFactory
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    url = f"{FOREXFACTORY_BASE_URL}?month=next"
    if verbose:
        print(f"Fetching next month's events from: {url}")
    
    return _fetch_and_save_events(url, verbose)

# Background task wrappers for scheduled execution
@anvil.server.callable
@anvil.server.background_task
def bg_fetch_tomorrow_events(verbose=VERBOSE_LOGGING):
    """
    Background task wrapper for fetch_tomorrow_events.
    Allows scheduling the task to run at specified times.
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    print("Starting background task: fetch_tomorrow_events")
    result = fetch_tomorrow_events(verbose=verbose)
    print("Completed background task: fetch_tomorrow_events")
    return result

@anvil.server.callable
@anvil.server.background_task
def bg_fetch_this_week_events(verbose=VERBOSE_LOGGING):
    """
    Background task wrapper for fetch_this_week_events.
    Allows scheduling the task to run at specified times.
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    print("Starting background task: fetch_this_week_events")
    result = fetch_this_week_events(verbose=verbose)
    print("Completed background task: fetch_this_week_events")
    return result

@anvil.server.callable
@anvil.server.background_task
def bg_fetch_next_week_events(verbose=VERBOSE_LOGGING):
    """
    Background task wrapper for fetch_next_week_events.
    Allows scheduling the task to run at specified times.
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    print("Starting background task: fetch_next_week_events")
    result = fetch_next_week_events(verbose=verbose)
    print("Completed background task: fetch_next_week_events")
    return result

@anvil.server.callable
@anvil.server.background_task
def bg_fetch_this_month_events(verbose=VERBOSE_LOGGING):
    """
    Background task wrapper for fetch_this_month_events.
    Allows scheduling the task to run at specified times.
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    print("Starting background task: fetch_this_month_events")
    result = fetch_this_month_events(verbose=verbose)
    print("Completed background task: fetch_this_month_events")
    return result

@anvil.server.callable
@anvil.server.background_task
def bg_fetch_next_month_events(verbose=VERBOSE_LOGGING):
    """
    Background task wrapper for fetch_next_month_events.
    Allows scheduling the task to run at specified times.
    
    Args:
        verbose: Whether to print detailed logs
    
    Returns:
        dict: Statistics about processed events
    """
    print("Starting background task: fetch_next_month_events")
    result = fetch_next_month_events(verbose=verbose)
    print("Completed background task: fetch_next_month_events")
    return result

@anvil.server.callable
@anvil.server.background_task
def refresh_all_calendars(verbose=False):
    """
    Refresh all calendar periods (tomorrow, this week, next week, this month, next month)
    with condensed logging output that only shows the final statistics.
    
    Args:
        verbose: Whether to print detailed logs
    
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
        if verbose:
            print(f"\nProcessing {period_name} calendar...")
        else:
            print(f"Processing {period_name}...")
        
        # Pass the verbose flag to the fetch function
        stats = fetch_function(verbose=verbose)
        
        # Add to combined totals
        combined_stats["total"] += stats["total"]
        combined_stats["existing"] += stats["existing"]
        combined_stats["new"] += stats["new"]
        
        # Store individual statistics
        combined_stats["details"][period_name] = stats
    
    # Always print combined statistics (even in non-verbose mode)
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
