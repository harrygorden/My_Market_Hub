import anvil.server
import anvil.http
from bs4 import BeautifulSoup
import datetime
import re
import pytz
from ..Shared_Functions import DB_Utils

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
        chicago_tz = pytz.timezone('America/Chicago')
        now = datetime.datetime.now(chicago_tz)
        
        # Calculate end date (10 days from now)
        end_date = now + datetime.timedelta(days=10)
        
        print(f"Retrieving USD events from {now.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Fetch the ForexFactory calendar page
        url = "https://www.forexfactory.com/calendar"
        print(f"Sending HTTP request to {url}")
        
        # Get the response and handle the StreamingMedia object properly
        response = anvil.http.request(url, json=False)
        
        # Check if response exists
        if not response:
            print("Failed to retrieve the calendar page")
            return False
        
        print("Successfully retrieved calendar page")
        
        # Convert the response to a string if it's a streaming object
        try:
            response_text = response.get_bytes().decode('utf-8')
        except AttributeError:
            # If it's already a string, use it as is
            response_text = response
            
        print(f"Response successfully processed")
        
        # Extract timezone information from the HTML
        site_timezone = None
        site_timezone_name = None
        
        # Look for the FF settings object in the HTML
        timezone_match = re.search(r"window\.FF\s*=\s*\{[^}]*timezone_name:\s*'([^']*)'", response_text)
        if timezone_match:
            site_timezone_name = timezone_match.group(1)
            try:
                site_timezone = pytz.timezone(site_timezone_name)
                print(f"Extracted site timezone: {site_timezone_name}")
            except Exception as e:
                print(f"Error setting site timezone: {e}")
                # Fall back to Chicago time if we can't parse the site timezone
                site_timezone = chicago_tz
                site_timezone_name = 'America/Chicago'
        else:
            print("Couldn't extract site timezone, using America/Chicago")
            site_timezone = chicago_tz
            site_timezone_name = 'America/Chicago'
        
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
                # Extract and format the date
                date_span = date_cell.find('span')
                if date_span:
                    date_text = date_span.text.strip()
                    # Parse date (format is like "Mon Mar 4")
                    try:
                        # Add current year since it's not in the date string
                        date_with_year = f"{date_text} {now.year}"
                        # Create a naive datetime object
                        parsed_date = datetime.datetime.strptime(date_with_year, "%a %b %d %Y")
                        
                        # Make it timezone-aware in the site's timezone
                        parsed_date = site_timezone.localize(parsed_date)
                        
                        # Convert to Chicago time
                        parsed_date = parsed_date.astimezone(chicago_tz)
                        
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
                            parsed_date = site_timezone.localize(naive_parsed).astimezone(chicago_tz)
                        
                        current_date = parsed_date.strftime("%Y-%m-%d")
                        print(f"Successfully parsed date: {current_date} (Chicago time)")
                    except Exception as e:
                        print(f"Error parsing date '{date_text}': {e}")
                        continue
            
            # Check if this is an event row
            if 'calendar__row' in row.get('class', []):
                # Skip if we don't have a date yet
                if not current_date:
                    continue
                
                # Extract the event data
                try:
                    # Get the currency
                    currency_cell = row.find('td', class_='calendar__cell calendar__currency')
                    currency = currency_cell.text.strip() if currency_cell else ''
                    
                    # Skip non-USD events
                    if currency != 'USD':
                        continue
                    
                    # Get the event time
                    time_cell = row.find('td', class_='calendar__cell calendar__time')
                    event_time = time_cell.text.strip() if time_cell else ''
                    
                    # Convert the event time to Chicago time if it's not empty
                    chicago_event_time = event_time
                    if event_time and event_time != '' and event_time.lower() != 'all day' and event_time.lower() != 'tentative':
                        try:
                            # Parse the time (format is like "8:30am")
                            # Handle 'Day X' format (like 'Day 2') or any other non-standard format
                            if re.match(r'^([0-9]{1,2}):([0-9]{2})(am|pm)$', event_time):
                                # Parse standard time format
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
                                chicago_datetime = full_datetime.astimezone(chicago_tz)
                                
                                # Format the time for output
                                chicago_event_time = chicago_datetime.strftime("%I:%M%p").lower()
                                
                                # Update the current_date if the date changed due to timezone conversion
                                if chicago_datetime.date() != event_date_obj.date():
                                    current_date = chicago_datetime.strftime("%Y-%m-%d")
                        except Exception as e:
                            print(f"Error converting time '{event_time}': {e}")
                            # Keep the original time if conversion fails
                    
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
                    
                    # Parse the event date to check if it's within our date range
                    try:
                        # Create a naive datetime object first
                        event_date_naive = datetime.datetime.strptime(current_date, "%Y-%m-%d")
                        # Make it timezone-aware with Chicago timezone
                        event_date = chicago_tz.localize(event_date_naive)
                        
                        # Skip events outside our target date range - both now timezone-aware in Chicago time
                        if event_date < now or event_date > end_date:
                            continue
                    except Exception as e:
                        print(f"Error checking event date range: {e}")
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
                        'timezone': 'America/Chicago'  # Add timezone info to the event data
                    }
                    
                    events.append(event_data)
                    
                    # Print USD events
                    print(f"USD Event: {current_date} {chicago_event_time} | {event_name} | Impact: {impact} | Forecast: {forecast} | Previous: {previous} (Chicago time)")
                    
                except Exception as e:
                    print(f"Error processing event row: {e}")
                    continue
        
        print(f"Extracted {len(events)} USD events within date range (all times in Chicago timezone)")
        
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
    chicago_tz = pytz.timezone('America/Chicago')
    now = datetime.datetime.now(chicago_tz)
    current_year = now.year
    current_month = now.month
    
    # Clear existing events for this month to avoid duplicates
    DB_Utils.clear_market_calendar_events_for_month(current_year, current_month)
    
    # Get events for the current month
    events = _process_calendar_for_month("https://www.forexfactory.com/calendar?month=this")
    
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
    chicago_tz = pytz.timezone('America/Chicago')
    now = datetime.datetime.now(chicago_tz)
    
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
    events = _process_calendar_for_month("https://www.forexfactory.com/calendar?month=next")
    
    # Save events to the marketcalendar table
    if events:
        saved_count = DB_Utils.save_multiple_market_calendar_events(events)
        next_month_name = datetime.date(next_year, next_month, 1).strftime('%B %Y')
        print(f"Saved {saved_count} events to marketcalendar table for {next_month_name}")
    else:
        next_month_name = datetime.date(next_year, next_month, 1).strftime('%B %Y')
        print(f"No events to save for {next_month_name}")
    
    return events


def _process_calendar_for_month(url):
    """
    Helper function to process the calendar data for a given month URL
    Filters for USD currency events only
    Returns a list of event dictionaries or False if an error occurs
    Ensures all dates/times are converted to America/Chicago timezone
    """
    try:
        print(f"Processing calendar data from {url}")
        
        # Fetch the ForexFactory calendar page for the month
        print(f"Sending HTTP request to {url}")
        
        # Get the response and handle the StreamingMedia object properly
        response = anvil.http.request(url, json=False)
        
        # Check if response exists
        if not response:
            print("Failed to retrieve the calendar page")
            return False
        
        print("Successfully retrieved calendar page")
        
        # Convert the response to a string if it's a streaming object
        try:
            response_text = response.get_bytes().decode('utf-8')
        except AttributeError:
            # If it's already a string, use it as is
            response_text = response
            
        print(f"Response successfully processed")
        
        # Extract timezone information from the HTML
        chicago_tz = pytz.timezone('America/Chicago')
        site_timezone = None
        site_timezone_name = None
        
        # Look for the FF settings object in the HTML
        timezone_match = re.search(r"window\.FF\s*=\s*\{[^}]*timezone_name:\s*'([^']*)'", response_text)
        if timezone_match:
            site_timezone_name = timezone_match.group(1)
            try:
                site_timezone = pytz.timezone(site_timezone_name)
                print(f"Extracted site timezone: {site_timezone_name}")
            except Exception as e:
                print(f"Error setting site timezone: {e}")
                # Fall back to Chicago time if we can't parse the site timezone
                site_timezone = chicago_tz
                site_timezone_name = 'America/Chicago'
        else:
            print("Couldn't extract site timezone, using America/Chicago")
            site_timezone = chicago_tz
            site_timezone_name = 'America/Chicago'
        
        # Parse the HTML content
        soup = BeautifulSoup(response_text, 'html.parser')
        
        # Find the calendar table
        calendar_table = soup.find('table', class_='calendar__table')
        
        if not calendar_table:
            print("Calendar table not found in the page")
            print(f"First 500 chars of response: {response_text[:500]}")
            return False
        
        print("Found calendar table in the HTML")
        
        # Get the current date and time in Chicago time
        now = datetime.datetime.now(chicago_tz)
        
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
                # Extract and format the date
                date_span = date_cell.find('span')
                if date_span:
                    date_text = date_span.text.strip()
                    # Parse date (format is like "Mon Mar 4")
                    try:
                        # Add current year since it's not in the date string
                        date_with_year = f"{date_text} {now.year}"
                        # Create a naive datetime object
                        parsed_date = datetime.datetime.strptime(date_with_year, "%a %b %d %Y")
                        
                        # Make it timezone-aware in the site's timezone
                        parsed_date = site_timezone.localize(parsed_date)
                        
                        # Convert to Chicago time
                        parsed_date = parsed_date.astimezone(chicago_tz)
                        
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
                            parsed_date = site_timezone.localize(naive_parsed).astimezone(chicago_tz)
                        
                        current_date = parsed_date.strftime("%Y-%m-%d")
                        print(f"Successfully parsed date: {current_date} (Chicago time)")
                    except Exception as e:
                        print(f"Error parsing date '{date_text}': {e}")
                        continue
            
            # Check if this is an event row
            if 'calendar__row' in row.get('class', []):
                # Skip if we don't have a date yet
                if not current_date:
                    continue
                
                # Extract the event data
                try:
                    # Get the currency
                    currency_cell = row.find('td', class_='calendar__cell calendar__currency')
                    currency = currency_cell.text.strip() if currency_cell else ''
                    
                    # Skip non-USD events
                    if currency != 'USD':
                        continue
                    
                    # Get the event time
                    time_cell = row.find('td', class_='calendar__cell calendar__time')
                    event_time = time_cell.text.strip() if time_cell else ''
                    
                    # Convert the event time to Chicago time if it's not empty
                    chicago_event_time = event_time
                    if event_time and event_time != '' and event_time.lower() != 'all day' and event_time.lower() != 'tentative':
                        try:
                            # Parse the time (format is like "8:30am")
                            # Handle 'Day X' format (like 'Day 2') or any other non-standard format
                            if re.match(r'^([0-9]{1,2}):([0-9]{2})(am|pm)$', event_time):
                                # Parse standard time format
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
                                chicago_datetime = full_datetime.astimezone(chicago_tz)
                                
                                # Format the time for output
                                chicago_event_time = chicago_datetime.strftime("%I:%M%p").lower()
                                
                                # Update the current_date if the date changed due to timezone conversion
                                if chicago_datetime.date() != event_date_obj.date():
                                    current_date = chicago_datetime.strftime("%Y-%m-%d")
                        except Exception as e:
                            print(f"Error converting time '{event_time}': {e}")
                            # Keep the original time if conversion fails
                    
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
                    
                    # Construct event data
                    event_data = {
                        'date': current_date,
                        'time': chicago_event_time,
                        'currency': currency,
                        'event': event_name,
                        'impact': impact,
                        'forecast': forecast,
                        'previous': previous,
                        'timezone': 'America/Chicago'  # Add timezone info to the event data
                    }
                    
                    events.append(event_data)
                    
                    # Print the USD event
                    print(f"USD Event: {current_date} {chicago_event_time} | {event_name} | Impact: {impact} | Forecast: {forecast} | Previous: {previous} (Chicago time)")
                    
                except Exception as e:
                    print(f"Error processing event row: {e}")
                    continue
        
        print(f"Extracted {len(events)} USD events for the month (all times in Chicago timezone)")
        return events
        
    except Exception as e:
        print(f"Error in _process_calendar_for_month: {e}")
        return False

# You can test these functions using the uplink with:
# anvil.server.call('retrieve_market_calendar_events_this_month')
# anvil.server.call('retrieve_market_calendar_events_next_month')
