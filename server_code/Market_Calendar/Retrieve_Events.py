import anvil.server
import anvil.http
from bs4 import BeautifulSoup
import datetime
import re
import pytz
from ..Shared_Functions import DB_Utils

def _retrieve_market_calendar_events_from_url(url, save_to_db=True, clear_existing=False, filter_currency="USD"):
    """
    Helper function to retrieve market calendar events from ForexFactory.com using a specific URL
    This function does the actual scraping work and is called by the public-facing functions
    
    Args:
        url (str): The ForexFactory calendar URL to scrape
        save_to_db (bool, optional): Whether to save events to the database. Default is True.
        clear_existing (bool, optional): Whether to clear existing events for the same dates. Default is False.
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
        
    Returns:
        list: A list of event dictionaries or False if an error occurred
    """
    print(f"Starting ForexFactory calendar scraping from: {url}")
    
    try:
        # Get the current date and time in Central Time
        chicago_tz = pytz.timezone('America/Chicago')
        now = datetime.datetime.now(chicago_tz)
        
        # Fetch the ForexFactory calendar page
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
        
        # Extract timezone information from the page
        site_timezone_offset = None
        site_timezone_name = None
        
        # Look for timezone information in the script tags
        timezone_pattern = re.compile(r"timezone:\s*'([^']+)'")
        timezone_name_pattern = re.compile(r"timezone_name:\s*'([^']+)'")
        
        # Find all script tags
        script_tags = BeautifulSoup(response_text, 'html.parser').find_all('script')
        
        for script in script_tags:
            if script.string:
                # Search for timezone offset
                tz_match = timezone_pattern.search(script.string)
                if tz_match:
                    site_timezone_offset = tz_match.group(1)
                    print(f"Found site timezone offset: {site_timezone_offset}")
                
                # Search for timezone name
                tz_name_match = timezone_name_pattern.search(script.string)
                if tz_name_match:
                    site_timezone_name = tz_name_match.group(1)
                    print(f"Found site timezone name: {site_timezone_name}")
                
                # Break if we found both
                if site_timezone_offset and site_timezone_name:
                    break
        
        # If we couldn't find the timezone info, assume UTC
        if not site_timezone_offset:
            print("Could not find timezone information, assuming UTC")
            site_timezone = pytz.UTC
        else:
            try:
                # Try to use the timezone name if available
                if site_timezone_name:
                    try:
                        site_timezone = pytz.timezone(site_timezone_name)
                        print(f"Using timezone from name: {site_timezone_name}")
                    except pytz.exceptions.UnknownTimeZoneError:
                        # Fall back to offset if name is invalid
                        hours_offset = int(site_timezone_offset)
                        site_timezone = pytz.FixedOffset(hours_offset * 60)
                        print(f"Using timezone from offset: {hours_offset} hours")
                else:
                    # Use the offset directly
                    hours_offset = int(site_timezone_offset)
                    site_timezone = pytz.FixedOffset(hours_offset * 60)
                    print(f"Using timezone from offset: {hours_offset} hours")
            except Exception as e:
                print(f"Error parsing timezone information: {e}, falling back to UTC")
                site_timezone = pytz.UTC
        
        print(f"Site timezone: {site_timezone}")
        print(f"Target timezone (Chicago): {chicago_tz}")
        
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
        last_time = None  # Track the last seen time to fill in missing times
        
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
                        
                        # Make it timezone-aware with the site's timezone
                        parsed_date = site_timezone.localize(parsed_date)
                        
                        # Convert to Chicago time
                        parsed_date_chicago = parsed_date.astimezone(chicago_tz)
                        
                        # Fix year if the parsed date is too far in the past (for December->January transition)
                        naive_now = now.replace(tzinfo=None)
                        naive_parsed = parsed_date_chicago.replace(tzinfo=None)
                        if (naive_now - naive_parsed).days > 300:
                            parsed_date = site_timezone.localize(
                                datetime.datetime(now.year + 1, parsed_date.month, parsed_date.day, 
                                                parsed_date.hour, parsed_date.minute, parsed_date.second)
                            )
                            # Convert to Chicago time again
                            parsed_date_chicago = parsed_date.astimezone(chicago_tz)
                        
                        current_date = parsed_date_chicago.strftime("%Y-%m-%d")
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
                    
                    # Skip this event if it's not for the filtered currency
                    if filter_currency and currency != filter_currency:
                        continue
                    
                    # Get the event time
                    time_cell = row.find('td', class_='calendar__cell calendar__time')
                    event_time = time_cell.text.strip() if time_cell else ''
                    
                    # If time is empty, use the previous time
                    if not event_time and last_time:
                        event_time = last_time
                        print(f"Using previous time '{event_time}' for event with missing time")
                    
                    # Convert the time to Chicago time if it's a valid time
                    if event_time and event_time != '' and ':' in event_time:
                        try:
                            # Parse the time string (format is like "8:30am")
                            # Check if time has am/pm indicator
                            time_pattern = re.compile(r'(\d+):(\d+)(am|pm)?', re.IGNORECASE)
                            time_match = time_pattern.match(event_time)
                            
                            if time_match:
                                hours = int(time_match.group(1))
                                minutes = int(time_match.group(2))
                                ampm = time_match.group(3)
                                
                                # Handle 12-hour format if am/pm is present
                                if ampm:
                                    if ampm.lower() == 'pm' and hours < 12:
                                        hours += 12
                                    elif ampm.lower() == 'am' and hours == 12:
                                        hours = 0
                                
                                # Create a datetime object for the event time in the site's timezone
                                # Use the parsed_date as the base to keep the same day
                                event_datetime = datetime.datetime.combine(
                                    parsed_date.date(),
                                    datetime.time(hours, minutes, 0)
                                )
                                event_datetime = site_timezone.localize(event_datetime)
                                
                                # Convert to Chicago time
                                event_datetime_chicago = event_datetime.astimezone(chicago_tz)
                                
                                # Format the time in Chicago timezone (to 12-hour format)
                                event_time = event_datetime_chicago.strftime("%-I:%M%p").lower()
                                
                                # Store this time for future events with missing times
                                last_time = event_time
                                
                                # Adjust the date if the day changed during conversion
                                if event_datetime_chicago.date() != parsed_date_chicago.date():
                                    print(f"Date changed during time conversion: {parsed_date_chicago.date()} -> {event_datetime_chicago.date()}")
                                    current_date = event_datetime_chicago.strftime("%Y-%m-%d")
                        except Exception as e:
                            print(f"Error converting time '{event_time}': {e}, keeping original time")
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
                        'time': event_time,
                        'currency': currency,
                        'event': event_name,
                        'impact': impact,
                        'forecast': forecast,
                        'previous': previous
                    }
                    
                    events.append(event_data)
                    
                    # Print the event
                    print(f"Event: {current_date} {event_time} | {currency} | {event_name} | Impact: {impact} | Forecast: {forecast} | Previous: {previous}")
                    
                except Exception as e:
                    print(f"Error processing event row: {e}")
                    continue
        
        print(f"Extracted {len(events)} total events from {url}")
        if filter_currency:
            print(f"Filtered to {len(events)} {filter_currency} events")
        
        # Save events to the database if requested
        if save_to_db and events:
            print(f"Saving {len(events)} events to marketcalendar table")
            save_results = DB_Utils.save_market_calendar_events(events, clear_existing=clear_existing)
            
            if save_results.get('error'):
                print(f"Error saving events to database: {save_results['error']}")
            else:
                print(f"Database save results: {save_results['added']} added, {save_results['updated']} updated, {save_results['skipped']} skipped")
        
        return events
        
    except Exception as e:
        print(f"Error in _retrieve_market_calendar_events_from_url: {e}")
        return False

@anvil.server.callable
def retrieve_market_calendar_events_this_month(save_to_db=True, clear_existing=False, filter_currency="USD"):
    """
    Retrieves market calendar events for the current month from ForexFactory.com
    
    Args:
        save_to_db (bool, optional): Whether to save events to the database. Default is True.
        clear_existing (bool, optional): Whether to clear existing events for the same dates. Default is False.
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
    
    Returns:
        list: A list of event dictionaries or False if an error occurred
    
    This function can be called via uplink for testing
    """
    url = "https://www.forexfactory.com/calendar?month=this"
    events = _retrieve_market_calendar_events_from_url(url, save_to_db=save_to_db, clear_existing=clear_existing, filter_currency=filter_currency)
    
    # Post-processing to fill in missing times
    if events:
        events = _fill_missing_times(events)
        print(f"Successfully retrieved {len(events)} events for this month")
    else:
        print("Failed to retrieve events for this month")
    
    return events

@anvil.server.callable
def retrieve_market_calendar_events_next_month(save_to_db=True, clear_existing=True, filter_currency="USD"):
    """
    Retrieves market calendar events for the next month from ForexFactory.com
    
    Args:
        save_to_db (bool, optional): Whether to save events to the database. Default is True.
        clear_existing (bool, optional): Whether to clear existing events for the same dates. Default is True.
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
    
    Returns:
        list: A list of event dictionaries or False if an error occurred
    
    This function can be scheduled to run on the first of each month
    """
    url = "https://www.forexfactory.com/calendar?month=next"
    events = _retrieve_market_calendar_events_from_url(url, save_to_db=save_to_db, clear_existing=clear_existing, filter_currency=filter_currency)
    
    # Post-processing to fill in missing times
    if events:
        events = _fill_missing_times(events)
        print(f"Successfully retrieved {len(events)} events for next month")
    else:
        print("Failed to retrieve events for next month")
    
    return events

@anvil.server.callable
def retrieve_market_calendar_events(filter_currency="USD"):
    """
    Legacy function that retrieves market calendar events for the current month
    This is kept for backward compatibility
    
    Args:
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
    
    Returns:
        list: A list of event dictionaries or False if an error occurred
    """
    print("WARNING: retrieve_market_calendar_events() is deprecated. Use retrieve_market_calendar_events_this_month() instead.")
    return retrieve_market_calendar_events_this_month(filter_currency=filter_currency)

def _fill_missing_times(events):
    """
    Post-processing function to fill in missing time values with the time from the previous event.
    This is more robust than trying to handle it during scraping.
    
    Args:
        events (list): List of event dictionaries
        
    Returns:
        list: Events with missing times filled in
    """
    print("Filling in missing time values...")
    
    # First sort events by date and original order
    # We'll add an index to preserve original order
    for i, event in enumerate(events):
        event['_index'] = i
    
    # Sort by date
    events_by_date = {}
    for event in events:
        date = event['date']
        if date not in events_by_date:
            events_by_date[date] = []
        events_by_date[date].append(event)
    
    # Process each date's events to fill in missing times
    for date, day_events in events_by_date.items():
        # Sort by original index to maintain original order
        day_events.sort(key=lambda e: e['_index'])
        
        # Fill in missing times with previous event's time
        last_time = None
        for event in day_events:
            if not event['time'] or event['time'] == '':
                if last_time:
                    print(f"Filling in missing time for event {event['event']} on {date} with {last_time}")
                    event['time'] = last_time
            else:
                last_time = event['time']
    
    # Flatten back to a list and remove the temporary index
    result = []
    for day_events in events_by_date.values():
        for event in day_events:
            del event['_index']
            result.append(event)
    
    # Sort back to original order
    result.sort(key=lambda e: events.index(e))
    
    print(f"Filled in times for events")
    
    # If events were already saved to the database, update them there
    if any(e.get('_saved_to_db', False) for e in events):
        print("Updating events in database with filled times...")
        DB_Utils.save_market_calendar_events(result, clear_existing=False)
    
    return result

# You can test these functions using the uplink with:
# anvil.server.call('retrieve_market_calendar_events_this_month')
# anvil.server.call('retrieve_market_calendar_events_next_month')
