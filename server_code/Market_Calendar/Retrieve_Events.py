import anvil.server
import anvil.http
from bs4 import BeautifulSoup
import datetime
import re
import pytz
from ..Shared_Functions import DB_Utils

# Constants
CHICAGO_TZ = pytz.timezone('America/Chicago')
FOREXFACTORY_BASE_URL = "https://www.forexfactory.com/calendar"
USD_CURRENCY = "USD"

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

def _extract_site_timezone(response_text):
    """Extract the timezone from site HTML and return timezone object"""
    # Look for the FF settings object in the HTML
    timezone_match = re.search(r"window\.FF\s*=\s*\{[^}]*timezone_name:\s*'([^']*)'", response_text)
    if timezone_match:
        site_timezone_name = timezone_match.group(1)
        try:
            site_timezone = pytz.timezone(site_timezone_name)
            print(f"Extracted site timezone: {site_timezone_name}")
            return site_timezone
        except Exception as e:
            print(f"Error setting site timezone: {e}")
    
    # Fall back to Chicago time if extraction fails
    print("Using America/Chicago timezone")
    return CHICAGO_TZ

def _parse_event_date(date_text, now, site_timezone):
    """Parse a date string and handle year transitions correctly"""
    try:
        # Add current year since it's not in the date string
        date_with_year = f"{date_text} {now.year}"
        # Create a naive datetime object
        parsed_date = datetime.datetime.strptime(date_with_year, "%a %b %d %Y")
        
        # Make it timezone-aware in the site's timezone
        parsed_date = site_timezone.localize(parsed_date)
        
        # Convert to Chicago time
        parsed_date = parsed_date.astimezone(CHICAGO_TZ)
        
        # Fix year if the parsed date is too far in the past (for December->January transition)
        naive_now = now.replace(tzinfo=None)
        naive_parsed = parsed_date.replace(tzinfo=None)
        if (naive_now - naive_parsed).days > 300:
            # Create a new date with the next year
            naive_parsed = datetime.datetime(
                now.year + 1, parsed_date.month, parsed_date.day, 
                parsed_date.hour, parsed_date.minute, parsed_date.second
            )
            # Convert to aware datetime in site timezone then to Chicago
            parsed_date = site_timezone.localize(naive_parsed).astimezone(CHICAGO_TZ)
        
        return parsed_date.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error parsing date '{date_text}': {e}")
        return None

def _parse_event_time(event_time, current_date, site_timezone):
    """Parse event time and convert to Chicago timezone"""
    # If the time is non-standard, return it as is
    if not event_time or event_time == '' or event_time.lower() == 'all day' or event_time.lower() == 'tentative':
        return event_time
        
    try:
        # Parse standard time format (like "8:30am")
        if re.match(r'^([0-9]{1,2}):([0-9]{2})(am|pm)$', event_time):
            time_obj = datetime.datetime.strptime(event_time, "%I:%M%p")
            
            # Create a full datetime by combining the date and time
            event_date_obj = datetime.datetime.strptime(current_date, "%Y-%m-%d")
            full_datetime = datetime.datetime.combine(
                event_date_obj.date(),
                time_obj.time()
            )
            
            # Add site timezone information
            full_datetime = site_timezone.localize(full_datetime)
            
            # Convert to Chicago time
            chicago_datetime = full_datetime.astimezone(CHICAGO_TZ)
            
            # Format the time for output
            return chicago_datetime.strftime("%I:%M%p").lower()
    except Exception as e:
        print(f"Error converting time '{event_time}': {e}")
    
    # Return original if parsing fails
    return event_time

def _extract_impact_level(impact_span):
    """
    Extract impact level from ForexFactory HTML span element
    Returns "High", "Medium", "Low", or "" (empty string if not found)
    """
    if not impact_span:
        return ""
    
    # Check the title attribute which contains text like "High Impact Expected"
    impact_title = impact_span.get('title', '')
    print(f"Processing impact span with title: '{impact_title}'")
    
    # Match exact wording from ForexFactory
    if impact_title == "High Impact Expected":
        return "High"
    elif impact_title == "Medium Impact Expected":
        return "Medium"
    elif impact_title == "Low Impact Expected":
        return "Low"
    
    # Fallback: Check for partial matches in case the exact wording changes
    if "high impact" in impact_title.lower():
        return "High"
    elif "medium impact" in impact_title.lower() or "med impact" in impact_title.lower():
        return "Medium"
    elif "low impact" in impact_title.lower():
        return "Low"
    
    # If title doesn't have impact info, check CSS classes
    # ForexFactory uses classes like icon--ff-impact-red for high impact
    class_list = impact_span.get('class', [])
    if not isinstance(class_list, list):
        class_list = [class_list]
    
    class_str = ' '.join([str(cls) for cls in class_list])
    print(f"Impact span classes: '{class_str}'")
    
    if "ff-impact-red" in class_str:
        return "High"
    elif "ff-impact-orange" in class_str:
        return "Medium" 
    elif "ff-impact-yel" in class_str:
        return "Low"
    
    print(f"Could not determine impact from: {impact_span}")
    return ""

def _extract_calendar_events(response_text):
    """
    Extract calendar events from HTML response
    
    Args:
        response_text: HTML content from ForexFactory
    
    Returns:
        List of event dictionaries
    """
    if not response_text:
        return []
    
    # Get current time in Chicago timezone for date parsing
    now = datetime.datetime.now(CHICAGO_TZ)
    
    # Extract site timezone
    site_timezone = _extract_site_timezone(response_text)
    
    # Parse the HTML content
    soup = BeautifulSoup(response_text, 'html.parser')
    
    # Find the calendar table
    calendar_table = soup.find('table', class_='calendar__table')
    
    if not calendar_table:
        print("Calendar table not found in the page")
        print(f"First 500 chars of response: {response_text[:500]}")
        return []
    
    print("Found calendar table in the HTML")
    
    # Extract events from the table
    events = []
    current_date = None
    
    # Find all table rows
    rows = calendar_table.find_all('tr')
    print(f"Found {len(rows)} rows in the calendar table")
    
    for row in rows:
        # Check if this is a date row
        date_cell = row.find('td', class_='calendar__cell calendar__date')
        if date_cell:
            date_span = date_cell.find('span')
            if date_span:
                date_text = date_span.text.strip()
                current_date = _parse_event_date(date_text, now, site_timezone)
                if current_date:
                    print(f"Parsed date: {current_date} (Chicago time)")
        
        # Check if this is an event row
        if 'calendar__row' in row.get('class', []) and current_date:
            try:
                # Get the currency
                currency_cell = row.find('td', class_='calendar__cell calendar__currency')
                currency = currency_cell.text.strip() if currency_cell else ''
                
                # Skip non-USD events
                if currency != USD_CURRENCY:
                    continue
                
                # Get the event time
                time_cell = row.find('td', class_='calendar__cell calendar__time')
                event_time = time_cell.text.strip() if time_cell else ''
                
                # Convert to Chicago timezone
                chicago_event_time = _parse_event_time(event_time, current_date, site_timezone)
                
                # Get the event name
                event_cell = row.find('td', class_='calendar__cell calendar__event')
                event_name = event_cell.text.strip() if event_cell else ''
                
                # Get impact level
                impact_cell = row.find('td', class_='calendar__cell calendar__impact')
                impact = ""
                if impact_cell:
                    # Extract the impact span element - this contains the impact indicator
                    # It should have a title attribute like "High Impact Expected"
                    impact_span = impact_cell.find('span', class_=lambda c: c and ('icon--ff-impact' in c or 'universal-impact' in c))
                    
                    if not impact_span:
                        # Try a more general approach if the specific class search fails
                        impact_span = impact_cell.find('span')
                    
                    if impact_span:
                        # Log the raw span for debugging
                        print(f"Found impact span: {impact_span}")
                        
                        # Use our helper function to determine impact level
                        impact = _extract_impact_level(impact_span)
                        
                        # Make sure impact is capitalized properly
                        if impact:
                            impact = impact.capitalize()
                
                # Log the final impact for debugging
                print(f"Final impact for '{event_name}': '{impact}'")
                
                # Get forecast value
                forecast_cell = row.find('td', class_='calendar__cell calendar__forecast')
                forecast = forecast_cell.text.strip() if forecast_cell else ''
                
                # Get previous value
                previous_cell = row.find('td', class_='calendar__cell calendar__previous')
                previous = previous_cell.text.strip() if previous_cell else ''
                
                # Save the event in our list
                event_data = {
                    'date': current_date,
                    'time': chicago_event_time,
                    'currency': currency,
                    'event': event_name,
                    'impact': impact,
                    'forecast': forecast,
                    'previous': previous,
                    'timezone': 'America/Chicago'
                }
                
                events.append(event_data)
                
            except Exception as e:
                print(f"Error extracting event data from row: {e}")
    
    print(f"Extracted {len(events)} USD events from the calendar")
    return events

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

# Server callable functions for different time ranges

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

# Legacy functions for backward compatibility
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
