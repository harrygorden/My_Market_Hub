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

def _extract_calendar_events(response_text, filter_date_range=None):
    """
    Extract calendar events from HTML response
    
    Args:
        response_text: HTML content from ForexFactory
        filter_date_range: Optional tuple of (start_date, end_date) to filter events
    
    Returns:
        List of event dictionaries
    """
    if not response_text:
        return False
    
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
        return False
    
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
                impact = ''
                if impact_cell:
                    impact_span = impact_cell.find('span')
                    if impact_span and 'impact' in impact_span.get('class', [])[0]:
                        impact_class = impact_span.get('class', [])[0]
                        # Extract impact level (high, medium, low)
                        impact = re.search(r'impact--(.*)', impact_class)
                        if impact:
                            impact = impact.group(1)
                
                # Get forecast value
                forecast_cell = row.find('td', class_='calendar__cell calendar__forecast')
                forecast = forecast_cell.text.strip() if forecast_cell else ''
                
                # Get previous value
                previous_cell = row.find('td', class_='calendar__cell calendar__previous')
                previous = previous_cell.text.strip() if previous_cell else ''
                
                # Filter by date range if provided
                if filter_date_range:
                    start_date, end_date = filter_date_range
                    event_date = datetime.datetime.strptime(current_date, "%Y-%m-%d").date()
                    if event_date < start_date or event_date > end_date:
                        continue
                
                # Construct event data
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
                print(f"Error processing event row: {e}")
                continue
    
    print(f"Extracted {len(events)} USD events (all times in Chicago timezone)")
    return events

# Server callable functions
@anvil.server.callable
@anvil.server.background_task
def retrieve_market_calendar_events():
    """
    Retrieves market calendar events from ForexFactory.com
    Filters for USD currency events for the next 10 days
    Prints results to the console
    Ensures all dates/times are converted to America/Chicago timezone
    Saves events to the marketcalendar Anvil table
    
    This function can be called via uplink for testing
    """
    print("Starting ForexFactory calendar scraping for USD events")
    
    try:
        # Get the current date and time in Chicago Time
        now = datetime.datetime.now(CHICAGO_TZ)
        
        # Calculate end date (10 days from now)
        end_date = now + datetime.timedelta(days=10)
        
        print(f"Retrieving USD events from {now.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Fetch and parse the calendar page
        response_text = _get_response_text(FOREXFACTORY_BASE_URL)
        if not response_text:
            return False
            
        # Extract events with date range filter
        events = _extract_calendar_events(
            response_text, 
            filter_date_range=(now.date(), end_date.date())
        )
        
        # Save events to the marketcalendar table
        if events:
            # Clear existing events for the date range to avoid duplicates
            start_date = datetime.datetime.strptime(events[0]['date'], '%Y-%m-%d').date()
            end_date_obj = datetime.datetime.strptime(events[-1]['date'], '%Y-%m-%d').date()
            DB_Utils.clear_market_calendar_events_for_date_range(start_date, end_date_obj)
            
            # Save the new events
            saved_count = DB_Utils.save_multiple_market_calendar_events(events)
            print(f"Saved {saved_count} events to marketcalendar table")
        else:
            print("No events to save to the database")
        
        return events
        
    except Exception as e:
        print(f"Error in retrieve_market_calendar_events: {e}")
        return False


@anvil.server.callable
@anvil.server.background_task
def retrieve_market_calendar_events_this_month():
    """
    Retrieves market calendar events from ForexFactory.com for the current month
    Filters for USD currency events only
    Prints results to the console
    Saves events to the marketcalendar Anvil table
    
    This function can be scheduled to run monthly
    """
    print("Starting ForexFactory calendar scraping for this month (USD events only)")
    
    # Get the current month and year
    now = datetime.datetime.now(CHICAGO_TZ)
    current_year = now.year
    current_month = now.month
    
    # Clear existing events for this month to avoid duplicates
    DB_Utils.clear_market_calendar_events_for_month(current_year, current_month)
    
    # Get events for the current month
    url = f"{FOREXFACTORY_BASE_URL}?month=this"
    response_text = _get_response_text(url)
    events = _extract_calendar_events(response_text)
    
    # Save events to the marketcalendar table
    if events:
        saved_count = DB_Utils.save_multiple_market_calendar_events(events)
        print(f"Saved {saved_count} events to marketcalendar table for {now.strftime('%B %Y')}")
    else:
        print(f"No events to save for {now.strftime('%B %Y')}")
    
    return events


@anvil.server.callable
@anvil.server.background_task
def retrieve_market_calendar_events_next_month():
    """
    Retrieves market calendar events from ForexFactory.com for the next month
    Filters for USD currency events only
    Prints results to the console
    Saves events to the marketcalendar Anvil table
    
    This function can be scheduled to run on the first of each month
    """
    print("Starting ForexFactory calendar scraping for next month (USD events only)")
    
    # Calculate next month and year
    now = datetime.datetime.now(CHICAGO_TZ)
    
    # Get next month and year
    if now.month == 12:
        next_month = 1
        next_year = now.year + 1
    else:
        next_month = now.month + 1
        next_year = now.year
    
    # Clear existing events for next month to avoid duplicates
    DB_Utils.clear_market_calendar_events_for_month(next_year, next_month)
    
    # Get events for the next month
    url = f"{FOREXFACTORY_BASE_URL}?month=next"
    response_text = _get_response_text(url)
    events = _extract_calendar_events(response_text)
    
    # Save events to the marketcalendar table
    if events:
        saved_count = DB_Utils.save_multiple_market_calendar_events(events)
        next_month_name = datetime.date(next_year, next_month, 1).strftime('%B %Y')
        print(f"Saved {saved_count} events to marketcalendar table for {next_month_name}")
    else:
        next_month_name = datetime.date(next_year, next_month, 1).strftime('%B %Y')
        print(f"No events to save for {next_month_name}")
    
    return events


# You can test these functions using the uplink with:
# anvil.server.call('retrieve_market_calendar_events_this_month')
# anvil.server.call('retrieve_market_calendar_events_next_month')
