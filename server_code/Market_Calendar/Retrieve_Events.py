import anvil.server
import anvil.http
from bs4 import BeautifulSoup
import datetime
import re
import pytz

@anvil.server.callable
def retrieve_market_calendar_events():
    """
    Retrieves market calendar events from ForexFactory.com
    Filters for events for the next 10 days
    Prints results to the console
    
    This function can be called via uplink for testing
    """
    print("Starting ForexFactory calendar scraping")
    
    try:
        # Get the current date and time in Central Time
        central_tz = pytz.timezone('US/Central')
        now = datetime.datetime.now(central_tz)
        
        # Calculate end date (10 days from now)
        end_date = now + datetime.timedelta(days=10)
        
        print(f"Retrieving events from {now.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
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
                        
                        # Make it timezone-aware with the same timezone as 'now'
                        parsed_date = central_tz.localize(parsed_date)
                        
                        # Fix year if the parsed date is too far in the past (for December->January transition)
                        naive_now = now.replace(tzinfo=None)
                        naive_parsed = parsed_date.replace(tzinfo=None)
                        if (naive_now - naive_parsed).days > 300:
                            parsed_date = central_tz.localize(
                                datetime.datetime(now.year + 1, parsed_date.month, parsed_date.day, 
                                                parsed_date.hour, parsed_date.minute, parsed_date.second)
                            )
                        
                        current_date = parsed_date.strftime("%Y-%m-%d")
                        print(f"Successfully parsed date: {current_date}")
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
                    
                    # Parse the event date to check if it's within our date range
                    try:
                        # Create a naive datetime object first
                        event_date_naive = datetime.datetime.strptime(current_date, "%Y-%m-%d")
                        # Make it timezone-aware with the same timezone as 'now'
                        event_date = central_tz.localize(event_date_naive)
                        
                        # Skip events outside our target date range - both now timezone-aware
                        if event_date < now or event_date > end_date:
                            continue
                    except Exception as e:
                        print(f"Error checking event date range: {e}")
                        continue
                    
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
                    
                    # Print all events
                    print(f"Event: {current_date} {event_time} | {currency} | {event_name} | Impact: {impact} | Forecast: {forecast} | Previous: {previous}")
                    
                except Exception as e:
                    print(f"Error processing event row: {e}")
                    continue
        
        print(f"Extracted {len(events)} total events within date range")
        return events
        
    except Exception as e:
        print(f"Error in retrieve_market_calendar_events: {e}")
        return False

# You can test this function using the uplink with:
# anvil.server.call('retrieve_market_calendar_events')
