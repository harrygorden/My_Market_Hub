import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.secrets
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import anvil.http
from bs4 import BeautifulSoup
import datetime
import logging
import re
import pytz

@anvil.server.callable
@anvil.server.background_task
def retrieve_market_calendar_events():
    """
    Retrieves market calendar events from ForexFactory.com
    Filters for USD currency events for the next 10 days
    Stores results in the marketcalendar Anvil table
    
    This function is designed to be run as a scheduled task in Anvil,
    recommended to run every Thursday at 5PM Central time.
    """
    logging.info("Starting ForexFactory calendar scraping")
    
    try:
        # Get the current date and time in Central Time
        central_tz = pytz.timezone('US/Central')
        now = datetime.datetime.now(central_tz)
        
        # Calculate end date (10 days from now)
        end_date = now + datetime.timedelta(days=10)
        
        logging.info(f"Retrieving events from {now.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Fetch the ForexFactory calendar page
        url = "https://www.forexfactory.com/calendar"
        response = anvil.http.request(url, json=False)
        
        if not response:
            logging.error("Failed to retrieve the calendar page")
            return False
        
        # Parse the HTML content
        soup = BeautifulSoup(response, 'html.parser')
        
        # Find the calendar table
        calendar_table = soup.find('table', class_='calendar__table')
        
        if not calendar_table:
            logging.error("Calendar table not found in the page")
            return False
        
        # Extract events from the table
        events = []
        current_date = None
        
        # Find all table rows
        rows = calendar_table.find_all('tr')
        
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
                        parsed_date = datetime.datetime.strptime(date_with_year, "%a %b %d %Y")
                        # Fix year if the parsed date is too far in the past (for December->January transition)
                        if (now - parsed_date).days > 300:
                            parsed_date = parsed_date.replace(year=now.year + 1)
                        current_date = parsed_date.strftime("%Y-%m-%d")
                    except Exception as e:
                        logging.error(f"Error parsing date '{date_text}': {e}")
                        continue
            
            # Check if this is an event row
            if 'calendar__row' in row.get('class', []):
                # Skip if we don't have a date yet
                if not current_date:
                    continue
                
                # Extract the event data
                try:
                    # Check if the currency is USD
                    currency_cell = row.find('td', class_='calendar__cell calendar__currency')
                    if not currency_cell or currency_cell.text.strip() != 'USD':
                        continue
                    
                    # Get the event time
                    time_cell = row.find('td', class_='calendar__cell calendar__time')
                    event_time = time_cell.text.strip() if time_cell else ''
                    
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
                    
                    # Get event ID if available
                    event_id = ''
                    try:
                        if event_cell:
                            link = event_cell.find('a')
                            if link and 'href' in link.attrs:
                                href = link.get('href')
                                # Extract ID from URL
                                id_match = re.search(r'event=(\d+)', href)
                                if id_match:
                                    event_id = id_match.group(1)
                    except:
                        pass
                    
                    # Parse the event date to check if it's within our date range
                    try:
                        event_date = datetime.datetime.strptime(current_date, "%Y-%m-%d")
                        # Convert to datetime with timezone for proper comparison
                        event_date = central_tz.localize(event_date)
                        
                        # Skip events outside our target date range
                        if event_date < now or event_date > end_date:
                            continue
                    except Exception as e:
                        logging.error(f"Error parsing event date: {e}")
                        continue
                    
                    # Construct event data
                    event_data = {
                        'ID': event_id,
                        'date': current_date,
                        'time': event_time,
                        'event': event_name,
                        'country': 'United States',  # Since we're filtering for USD
                        'currency': 'USD',
                        'impact': impact,
                        'forecast': forecast,
                        'previous': previous
                    }
                    
                    events.append(event_data)
                    
                except Exception as e:
                    logging.error(f"Error processing event row: {e}")
                    continue
        
        # Save events to the marketcalendar table
        save_events_to_table(events)
        
        logging.info(f"Successfully retrieved and processed {len(events)} USD market events")
        return True
        
    except Exception as e:
        logging.error(f"Error in retrieve_market_calendar_events: {e}")
        return False

def save_events_to_table(events):
    """
    Save the scraped events to the marketcalendar Anvil table
    Handles duplicate checking to avoid adding the same event twice
    """
    if not events:
        logging.info("No events to save")
        return
    
    try:
        # Get the marketcalendar table
        calendar_table = app_tables.marketcalendar
        
        # Get existing events to check for duplicates
        existing_events = {}
        for row in calendar_table.search():
            # Create a unique key for each event based on date, time, and event name
            key = f"{row['date']}_{row['time']}_{row['event']}"
            existing_events[key] = row
        
        # Track statistics
        added_count = 0
        updated_count = 0
        skipped_count = 0
        
        # Add new events and update existing ones
        for event in events:
            try:
                # Create a unique key for this event
                key = f"{event['date']}_{event['time']}_{event['event']}"
                
                # Check if this event already exists
                if key in existing_events:
                    # Event exists - update it if needed
                    existing_row = existing_events[key]
                    
                    # Check if any fields need updating
                    needs_update = False
                    for field in ['impact', 'forecast', 'previous']:
                        if field in event and existing_row[field] != event[field]:
                            needs_update = True
                    
                    # Update if needed
                    if needs_update:
                        # Update only fields that might change
                        existing_row['impact'] = event['impact']
                        existing_row['forecast'] = event['forecast']
                        existing_row['previous'] = event['previous']
                        updated_count += 1
                    else:
                        skipped_count += 1
                else:
                    # New event - add it
                    calendar_table.add_row(**event)
                    added_count += 1
            except Exception as e:
                logging.error(f"Error saving event {event.get('event', '')}: {e}")
        
        logging.info(f"Calendar update complete: {added_count} added, {updated_count} updated, {skipped_count} unchanged")
        
    except Exception as e:
        logging.error(f"Error saving events to table: {e}")
