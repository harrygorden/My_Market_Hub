import anvil.server
import anvil.http
from bs4 import BeautifulSoup
import datetime
import re
import pytz
from ..Shared_Functions import DB_Utils
import calendar

def _retrieve_market_calendar_events_from_url(url, save_to_db=True, clear_existing=False, filter_currency="USD"):
    """
    Helper function to retrieve market calendar events from a given ForexFactory.com URL
    
    Args:
        url (str): The ForexFactory.com calendar URL to scrape
        save_to_db (bool, optional): Whether to save events to the database. Default is True.
        clear_existing (bool, optional): Whether to clear existing events for the same dates. Default is False.
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
    
    Returns:
        list: A list of event dictionaries or False if an error occurred
    """
    print(f"Retrieving market calendar events from {url}")
    
    try:
        # Get the calendar page
        response = anvil.http.request(url, json=False)
        
        # Parse the HTML content
        soup = BeautifulSoup(response, 'html.parser')
        
        # Find the calendar table
        table = soup.find('table', class_='calendar__table')
        
        if not table:
            print("No calendar table found on the page. The page structure may have changed.")
            return False
        
        # Find all rows in the table
        rows = table.find_all('tr')
        
        # Initialize variables
        events = []
        current_date = None
        last_time = None
        
        # Define timezone for Chicago (America/Chicago)
        chicago_tz = pytz.timezone('America/Chicago')
        
        # Initialize site timezone (will be determined dynamically from the page if possible)
        site_timezone = pytz.timezone('GMT')  # Default to GMT if we can't determine it
        
        # Try to determine the site timezone from the page
        timezone_element = soup.find('div', class_='calendar__timezone')
        if timezone_element:
            timezone_text = timezone_element.text.strip()
            print(f"Found timezone information: {timezone_text}")
            
            # Extract timezone information
            # Example format: "Timezone: GMT (+00:00)"
            timezone_pattern = re.search(r'GMT\s*\(([+-])(\d{2}):(\d{2})\)', timezone_text)
            if timezone_pattern:
                # Extract the timezone offset
                sign = timezone_pattern.group(1)
                hours = int(timezone_pattern.group(2))
                minutes = int(timezone_pattern.group(3))
                
                # Determine the total offset in hours
                offset = hours + (minutes / 60)
                if sign == '-':
                    offset = -offset
                
                # Try to find a matching timezone
                # Note: This is approximate and may not account for DST changes
                for tz_name in pytz.all_timezones:
                    tz = pytz.timezone(tz_name)
                    now = datetime.datetime.now(tz)
                    tz_offset = now.utcoffset().total_seconds() / 3600
                    
                    if abs(tz_offset - offset) < 0.01 and 'GMT' in tz_name:
                        site_timezone = tz
                        print(f"Matched site timezone to: {tz_name}")
                        break
            
            # Fallback if we couldn't match a timezone
            if site_timezone == pytz.timezone('GMT'):
                print("Could not determine exact timezone, using GMT as fallback")
        
        # Process each row
        for row in rows:
            # Check if this is a date row
            date_cell = row.find('td', class_='calendar__cell calendar__date')
            
            if date_cell:
                # Extract the date
                date_text = date_cell.text.strip()
                month_year = date_cell.find('span', class_='date').text.strip()
                
                # The date format can vary but is typically like "Mon Nov 13"
                # We need to parse this and add the year
                date_match = re.match(r'([A-Za-z]+)\s+([A-Za-z]+)\s+(\d+)', date_text)
                if date_match:
                    day_name = date_match.group(1)  # Not used, but extracted for completeness
                    month_name = date_match.group(2)
                    day = int(date_match.group(3))
                    
                    # Parse the month and year
                    month_match = re.match(r'([A-Za-z]+)\s+\'?(\d+)', month_year)
                    if month_match:
                        month_abbr = month_match.group(1)
                        year_abbr = month_match.group(2)
                        
                        # Handle two-digit years
                        if len(year_abbr) == 2:
                            year = 2000 + int(year_abbr)
                        else:
                            year = int(year_abbr)
                        
                        # Convert month name to number (1-12)
                        month = list(calendar.month_abbr).index(month_abbr[:3].title())
                        
                        # Create the date string in ISO format
                        current_date = f"{year}-{month:02d}-{day:02d}"
                        
                        # For timezone conversion, create a datetime object in the site's timezone
                        parsed_date = datetime.datetime(year, month, day, 0, 0, 0)
                        parsed_date = site_timezone.localize(parsed_date)
                        
                        # Convert to Chicago time
                        parsed_date_chicago = parsed_date.astimezone(chicago_tz)
                        
                        # Check if the day changed due to timezone difference
                        if parsed_date_chicago.date() != parsed_date.date():
                            print(f"Date changed due to timezone difference: {parsed_date.date()} -> {parsed_date_chicago.date()}")
                            current_date = parsed_date_chicago.strftime("%Y-%m-%d")
                        
                        print(f"Processing events for {current_date}")
                    else:
                        print(f"Could not parse month and year from '{month_year}'")
                        current_date = None
                else:
                    print(f"Could not parse date from '{date_text}'")
                    current_date = None
            
            # Check if this is an event row
            if row.has_attr('class') and 'calendar__row' in row['class'] and current_date:
                # Get the currency
                currency_cell = row.find('td', class_='calendar__cell calendar__currency')
                currency = currency_cell.text.strip() if currency_cell else ''
                
                # Skip non-matching currencies
                if filter_currency and currency != filter_currency:
                    continue
                
                try:
                    # Get the event time
                    time_cell = row.find('td', class_='calendar__cell calendar__time')
                    event_time = time_cell.text.strip() if time_cell else ''
                    
                    # Save original time for debugging
                    original_time = event_time
                    
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
                                
                                # Adjust the date if the day changed during conversion
                                if event_datetime_chicago.date() != parsed_date_chicago.date():
                                    print(f"Date changed during time conversion: {parsed_date_chicago.date()} -> {event_datetime_chicago.date()}")
                                    current_date = event_datetime_chicago.strftime("%Y-%m-%d")
                                    
                                print(f"Converted time '{original_time}' to '{event_time}' (Chicago time)")
                        except Exception as e:
                            print(f"Error converting time '{event_time}': {e}, keeping original time")
                            # Keep the original time if conversion fails
                    
                    # Get the event name
                    event_cell = row.find('td', class_='calendar__cell calendar__event')
                    event_name = event_cell.text.strip() if event_cell else ''
                    
                    # Get the impact
                    impact_cell = row.find('td', class_='calendar__cell calendar__impact')
                    impact = None
                    if impact_cell:
                        impact_span = impact_cell.find('span')
                        if impact_span and impact_span.has_attr('class'):
                            impact_class = impact_span['class'][0]
                            # Extract impact level from class name (e.g., "sentiment--bull-hi" -> "high")
                            if 'bull-hi' in impact_class:
                                impact = 'high'
                            elif 'bull-md' in impact_class:
                                impact = 'medium'
                            elif 'bull-lo' in impact_class:
                                impact = 'low'
                    
                    # Get the forecast value
                    forecast_cell = row.find('td', class_='calendar__cell calendar__forecast')
                    forecast = forecast_cell.text.strip() if forecast_cell else ''
                    
                    # Get the previous value
                    previous_cell = row.find('td', class_='calendar__cell calendar__previous')
                    previous = previous_cell.text.strip() if previous_cell else ''
                    
                    # Add the event to our list
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
                    
                except Exception as e:
                    print(f"Error processing event row: {e}")
        
        # Save events to the database if requested
        if save_to_db and events:
            print(f"Saving {len(events)} events to marketcalendar table")
            save_results = DB_Utils.save_market_calendar_events(events, clear_existing=clear_existing)
            
            if save_results.get('error'):
                print(f"Error saving events to database: {save_results['error']}")
            else:
                print(f"Database save results: {save_results['added']} added, {save_results['updated']} updated, {save_results['skipped']} skipped")
        
        # Return the list of events
        return events
    
    except Exception as e:
        print(f"Error retrieving market calendar events: {e}")
        return False

@anvil.server.background_task
def retrieve_market_calendar_events_this_month(save_to_db=True, clear_existing=False, filter_currency="USD"):
    """
    Retrieves market calendar events for the current month from ForexFactory.com
    
    This function runs as a background task to prevent timeouts during scraping.
    
    Args:
        save_to_db (bool, optional): Whether to save events to the database. Default is True.
        clear_existing (bool, optional): Whether to clear existing events for the same dates. Default is False.
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
    
    Returns:
        list: A list of event dictionaries or False if an error occurred
    
    This function can be called via uplink for testing
    """
    url = "https://www.forexfactory.com/calendar?month=this"
    
    # Step 1: Get raw events from the website without saving to database
    print(f"Step 1: Retrieving events from {url}")
    events = _retrieve_market_calendar_events_from_url(url, save_to_db=False, clear_existing=False, filter_currency=filter_currency)
    
    if not events:
        print("Failed to retrieve events for this month")
        return False
        
    # Step 2: Post-process to fill in missing times
    print(f"Step 2: Filling in missing times for {len(events)} events")
    processed_events = _fill_missing_times(events)
    print(f"Successfully processed {len(processed_events)} events for this month")
    
    # Step 3: Save processed events to database if requested
    if save_to_db:
        print(f"Step 3: Saving {len(processed_events)} fully processed events to database")
        save_results = DB_Utils.save_market_calendar_events(processed_events, clear_existing=clear_existing)
        print(f"Database save results: {save_results}")
    
    return processed_events

@anvil.server.background_task
def retrieve_market_calendar_events_next_month(save_to_db=True, clear_existing=True, filter_currency="USD"):
    """
    Retrieves market calendar events for the next month from ForexFactory.com
    
    This function runs as a background task to prevent timeouts during scraping.
    
    Args:
        save_to_db (bool, optional): Whether to save events to the database. Default is True.
        clear_existing (bool, optional): Whether to clear existing events for the same dates. Default is True.
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
    
    Returns:
        list: A list of event dictionaries or False if an error occurred
    
    This function can be scheduled to run on the first of each month
    """
    url = "https://www.forexfactory.com/calendar?month=next"
    
    # Step 1: Get raw events from the website without saving to database
    print(f"Step 1: Retrieving events from {url}")
    events = _retrieve_market_calendar_events_from_url(url, save_to_db=False, clear_existing=False, filter_currency=filter_currency)
    
    if not events:
        print("Failed to retrieve events for next month")
        return False
        
    # Step 2: Post-process to fill in missing times
    print(f"Step 2: Filling in missing times for {len(events)} events")
    processed_events = _fill_missing_times(events)
    print(f"Successfully processed {len(processed_events)} events for next month")
    
    # Step 3: Save processed events to database if requested
    if save_to_db:
        print(f"Step 3: Saving {len(processed_events)} fully processed events to database")
        save_results = DB_Utils.save_market_calendar_events(processed_events, clear_existing=clear_existing)
        print(f"Database save results: {save_results}")
    
    return processed_events

@anvil.server.background_task
def retrieve_market_calendar_events(filter_currency="USD"):
    """
    Legacy function that calls retrieve_market_calendar_events_this_month
    
    This function is kept for backwards compatibility with existing code.
    It now runs as a background task to prevent timeouts.
    
    Args:
        filter_currency (str, optional): Only return events for this currency. Default is "USD".
    
    Returns:
        list: A list of event dictionaries
    """
    print("retrieve_market_calendar_events called (legacy function)")
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
    
    # Create a copy of the events to avoid modifying the original
    processed_events = []
    for event in events:
        processed_events.append(event.copy())
    
    # Sort events by date for processing
    events_by_date = {}
    for event in processed_events:
        date = event['date']
        if date not in events_by_date:
            events_by_date[date] = []
        events_by_date[date].append(event)
    
    # Process each date's events to fill in missing times
    filled_count = 0
    for date, day_events in events_by_date.items():
        print(f"Processing events for date: {date}")
        
        # Process events in order for each day
        last_time = None
        for i, event in enumerate(day_events):
            print(f"  Event {i+1}: {event['event']}, Time: '{event['time']}'")
            
            # If time is missing but we have a previous time, fill it in
            if (not event['time'] or event['time'] == '') and last_time:
                event['time'] = last_time
                print(f"    Filled missing time with: {last_time}")
                filled_count += 1
            # If we have a time, update our "last time" tracker
            elif event['time'] and event['time'] != '':
                last_time = event['time']
                print(f"    Recorded time: {last_time}")
    
    print(f"Filled in times for {filled_count} events")
    
    # Flatten back to a list
    result = []
    for day_events in events_by_date.values():
        for event in day_events:
            result.append(event)
    
    return result

# You can test these functions using the uplink with:
# events = anvil.server.launch_background_task('retrieve_market_calendar_events_this_month').wait_for_result()
