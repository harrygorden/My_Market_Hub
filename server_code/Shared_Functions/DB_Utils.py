import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.secrets
import anvil.server
import datetime

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
# @anvil.server.callable
# def say_hello(name):
#   print("Hello, " + name + "!")
#   return 42
#

@anvil.server.callable
def save_market_calendar_event(event_data, verbose=True):
    """
    Save a single market calendar event to the marketcalendar Anvil table
    
    Args:
        event_data (dict): Dictionary containing event details
            - date (str): Event date in YYYY-MM-DD format
            - time (str): Event time (original time from ForexFactory)
            - currency (str): Currency code (e.g., 'USD')
            - event (str): Event name/description
            - impact (str): Impact level (high, medium, low)
            - forecast (str): Forecast value
            - previous (str): Previous value
            - timezone (str): The timezone of the source website
        verbose (bool): Whether to print detailed logs
            
    Returns:
        row: The newly created or updated table row
    """
    try:
        # Debug the incoming event data with special focus on the impact
        if verbose:
            print(f"Processing event: {event_data['event']} on {event_data['date']}")
            print(f"Impact value being saved: '{event_data.get('impact', '')}'")
        
        # Convert date string to datetime.date object
        event_date = datetime.datetime.strptime(event_data['date'], '%Y-%m-%d').date()
        
        # Create a unique event identifier based on date, time, and event name
        # This should prevent duplicate events even from different sources
        existing_events = app_tables.marketcalendar.search(
            date=event_date,
            event=event_data['event']
        )
        
        # Convert to list for checking if there are any matching events
        existing_events_list = list(existing_events)
        
        # Additional check for time to handle potential time format differences
        existing_event = None
        for event in existing_events_list:
            # Direct match
            if event['time'] == event_data['time']:
                existing_event = event
                break
                
            # Handle case where one might be "10:00am" and the other "10:00 am" or similar variants
            if event['time'] and event_data['time']:
                # Normalize times by removing spaces and converting to lowercase
                normalized_db_time = event['time'].lower().replace(' ', '')
                normalized_new_time = event_data['time'].lower().replace(' ', '')
                
                if normalized_db_time == normalized_new_time:
                    existing_event = event
                    break
        
        if existing_event:
            # Update existing event with new data, preserving the original if new data is empty
            updates = {}
            
            # Only update fields if the new data has a non-empty value
            if event_data.get('impact') and event_data['impact'] != existing_event['impact']:
                updates['impact'] = event_data['impact']
                if verbose:
                    print(f"Updating impact from '{existing_event['impact']}' to '{event_data['impact']}'")
            
            if event_data.get('forecast') and event_data['forecast'] != existing_event['forecast']:
                updates['forecast'] = event_data['forecast']
                
            if event_data.get('previous') and event_data['previous'] != existing_event['previous']:
                updates['previous'] = event_data['previous']
                
            # Only update if we have changes
            if updates:
                existing_event.update(**updates)
                if verbose:
                    print(f"Updated existing event: {event_data['event']} on {event_data['date']} at {event_data['time']}")
                    print(f"New impact value in database: '{existing_event['impact']}'")
            elif verbose:
                print(f"No changes needed for: {event_data['event']} on {event_data['date']} at {event_data['time']}")
                
            return existing_event
        else:
            # Create new event
            if verbose:
                print(f"Creating new event with impact: '{event_data.get('impact', '')}'")
            new_event = app_tables.marketcalendar.add_row(
                date=event_date,
                time=event_data['time'],
                event=event_data['event'],
                currency=event_data['currency'],
                impact=event_data.get('impact', ''),
                forecast=event_data.get('forecast', ''),
                previous=event_data.get('previous', '')
            )
            if verbose:
                print(f"Added new event: {event_data['event']} on {event_data['date']} at {event_data['time']}")
                print(f"Impact value saved to database: '{new_event['impact']}'")
            return new_event
    
    except Exception as e:
        print(f"Error saving market calendar event: {e}")
        return None

@anvil.server.callable
def save_multiple_market_calendar_events(events_list, verbose=True):
    """
    Save multiple market calendar events to the marketcalendar Anvil table
    
    Args:
        events_list (list): List of event dictionaries
        verbose (bool): Whether to print detailed logs
        
    Returns:
        dict: Statistics about processed events containing:
            - total: Total number of events processed
            - existing: Number of existing events (skipped or updated)
            - new: Number of newly added events
    """
    if not events_list:
        if verbose:
            print("No events to save")
        return {"total": 0, "existing": 0, "new": 0}
    
    if verbose:
        print(f"Processing {len(events_list)} events for saving to database")
    
    stats = {
        "total": len(events_list),
        "existing": 0,
        "new": 0
    }
    
    for event in events_list:
        # Check if this event already exists before saving
        event_date = datetime.datetime.strptime(event['date'], '%Y-%m-%d').date()
        existing_events = app_tables.marketcalendar.search(
            date=event_date,
            event=event['event']
        )
        
        # Check for time match using the same logic as in save_market_calendar_event
        existing_event = None
        for db_event in existing_events:
            # Direct match or normalized time match
            if (db_event['time'] == event['time'] or 
                (db_event['time'] and event['time'] and
                 db_event['time'].lower().replace(' ', '') == event['time'].lower().replace(' ', ''))):
                existing_event = db_event
                break
        
        result = save_market_calendar_event(event, verbose)
        
        if result:
            if existing_event:
                stats["existing"] += 1
            else:
                stats["new"] += 1
    
    if verbose:
        print(f"Event processing statistics:")
        print(f"Total Scraped Events: {stats['total']}")
        print(f"Skipped (existing) events: {stats['existing']}")
        print(f"Newly added events: {stats['new']}")
    
    return stats

@anvil.server.callable
def clear_market_calendar_events_for_date_range(start_date, end_date):
    """
    Remove market calendar events for a specific date range
    
    Args:
        start_date (datetime.date): Start date (inclusive)
        end_date (datetime.date): End date (inclusive)
        
    Returns:
        int: Number of rows deleted
    """
    try:
        # Find events in the date range
        events_to_delete = app_tables.marketcalendar.search(
            q.between(app_tables.marketcalendar.date, start_date, end_date)
        )
        
        # Count events
        count = len(list(events_to_delete))
        
        # Delete each event
        for event in events_to_delete:
            event.delete()
        
        print(f"Cleared {count} events from {start_date} to {end_date}")
        return count
    
    except Exception as e:
        print(f"Error clearing market calendar events: {e}")
        return 0

@anvil.server.callable
def clear_market_calendar_events_for_month(year, month):
    """
    Remove market calendar events for a specific month
    
    Args:
        year (int): Year
        month (int): Month (1-12)
        
    Returns:
        int: Number of rows deleted
    """
    try:
        # Calculate first and last day of the month
        first_day = datetime.date(year, month, 1)
        
        # Calculate last day (first day of next month - 1 day)
        if month == 12:
            last_day = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
        else:
            last_day = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
        
        return clear_market_calendar_events_for_date_range(first_day, last_day)
    
    except Exception as e:
        print(f"Error clearing market calendar events for month: {e}")
        return 0

@anvil.server.callable
def get_market_calendar_events_for_date_range(start_date, end_date):
    """
    Retrieve market calendar events for a specific date range
    
    Args:
        start_date (datetime.date): Start date (inclusive)
        end_date (datetime.date): End date (inclusive)
        
    Returns:
        list: List of event dictionaries
    """
    try:
        # Find events in the date range
        events = app_tables.marketcalendar.search(
            q.between(app_tables.marketcalendar.date, start_date, end_date)
        )
        
        # Convert to list of dictionaries for easier handling
        event_list = []
        for event in events:
            event_dict = {
                'date': event['date'].strftime('%Y-%m-%d'),
                'time': event['time'],
                'event': event['event'],
                'currency': event['currency'],
                'impact': event['impact'],
                'forecast': event['forecast'],
                'previous': event['previous']
            }
            event_list.append(event_dict)
        
        return event_list
        
    except Exception as e:
        print(f"Error retrieving market calendar events: {e}")
        return []

@anvil.server.callable
def get_market_calendar_events_by_impact(impact_level, start_date=None, end_date=None):
    """
    Retrieve market calendar events filtered by impact level and optional date range
    
    Args:
        impact_level (str): Impact level to filter by (high, medium, low)
        start_date (datetime.date, optional): Start date (inclusive)
        end_date (datetime.date, optional): End date (inclusive)
        
    Returns:
        list: List of event dictionaries
    """
    try:
        # Create query filters
        filters = [q.equal(app_tables.marketcalendar.impact, impact_level)]
        
        # Add date filters if provided
        if start_date and end_date:
            filters.append(q.between(app_tables.marketcalendar.date, start_date, end_date))
        elif start_date:
            filters.append(q.greater_than_or_equal_to(app_tables.marketcalendar.date, start_date))
        elif end_date:
            filters.append(q.less_than_or_equal_to(app_tables.marketcalendar.date, end_date))
        
        # Execute the search with filters
        events = app_tables.marketcalendar.search(*filters)
        
        # Convert to list of dictionaries
        event_list = []
        for event in events:
            event_dict = {
                'date': event['date'].strftime('%Y-%m-%d'),
                'time': event['time'],
                'event': event['event'],
                'currency': event['currency'],
                'impact': event['impact'],
                'forecast': event['forecast'],
                'previous': event['previous']
            }
            event_list.append(event_dict)
            
        return event_list
        
    except Exception as e:
        print(f"Error retrieving market calendar events by impact: {e}")
        return []

@anvil.server.callable
def get_market_calendar_events_with_timezone(start_date, end_date, target_timezone="UTC"):
    """
    Get market calendar events for the specified date range and timezone
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
        target_timezone (str, optional): Target timezone. Defaults to "UTC".
        
    Returns:
        list: List of event dictionaries with date, time, event, impact, forecast, previous
    """
    import datetime
    import pytz
    from anvil.tables import app_tables
    
    print(f"Type of start_date: {type(start_date)}")
    print(f"Value of start_date: {start_date}")
    print(f"Type of end_date: {type(end_date)}")
    print(f"Value of end_date: {end_date}")
    print(f"Target timezone: {target_timezone}")
    
    # Convert string dates to datetime objects
    if isinstance(start_date, str):
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        
    # Get timezone object
    if target_timezone == "Eastern":
        tz = pytz.timezone("America/New_York")
    elif target_timezone == "Central":
        tz = pytz.timezone("America/Chicago")
    elif target_timezone == "Mountain":
        tz = pytz.timezone("America/Denver")
    elif target_timezone == "Pacific":
        tz = pytz.timezone("America/Los_Angeles")
    else:
        tz = pytz.timezone("UTC")
    
    # Get all rows from marketcalendar table
    rows = app_tables.marketcalendar.search()
    
    # Format for debug
    print(f"Retrieved {len(rows)} total rows from marketcalendar")

    # Filter rows by date range - make sure everything is a date object for comparison
    filtered_rows = []
    for row in rows:
        row_date = row['date']
        if isinstance(row_date, str):
            # Convert string to date
            try:
                row_date = datetime.datetime.strptime(row_date, "%Y-%m-%d").date()
            except:
                # Skip if conversion fails
                continue
        
        # Check if the date is within the range
        if start_date <= row_date <= end_date:
            filtered_rows.append(row)
    
    print(f"Filtered to {len(filtered_rows)} rows in date range {start_date} to {end_date}")
    
    # Format events for return
    events = []
    for row in filtered_rows:
        # Some debug for the first event
        if len(events) == 0:
            print(f"First event in range: Date={row['date']}, Event={row['event']}")
        
        # Convert time from UTC to target timezone
        time_str = row['time']
        converted_time = time_str  # Initialize the variable with the original value as a fallback
        try:
            # Parse the time string to create a datetime object
            # Assume times are stored in 12-hour format with AM/PM
            time_format = "%I:%M %p"  # e.g., "08:30 AM"
            
            # First extract just the time part
            if time_str and isinstance(time_str, str):
                # Create a full datetime using both the date and time
                date_str = row['date'].strftime("%Y-%m-%d") if hasattr(row['date'], 'strftime') else str(row['date'])
                datetime_str = f"{date_str} {time_str}"
                
                # Parse the full datetime string
                try:
                    # Try with 12-hour format first
                    dt = datetime.datetime.strptime(datetime_str, f"%Y-%m-%d {time_format}")
                    # Make datetime timezone aware (assume UTC)
                    utc_dt = pytz.UTC.localize(dt)
                    
                    # Convert to target timezone
                    converted_dt = utc_dt.astimezone(tz)
                    
                    # Format back to time string
                    converted_time = converted_dt.strftime(time_format)
                except ValueError:
                    try:
                        # If that fails, try 24-hour format
                        dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
                        # Make datetime timezone aware (assume UTC)
                        utc_dt = pytz.UTC.localize(dt)
                        
                        # Convert to target timezone
                        converted_dt = utc_dt.astimezone(tz)
                        
                        # Format back to time string
                        converted_time = converted_dt.strftime(time_format)
                    except ValueError:
                        # If all parsing fails, use the original time string
                        print(f"Could not parse time: {time_str}")
                        # converted_time already initialized
        except Exception as e:
            print(f"Error converting time {time_str}: {str(e)}")
            # converted_time already initialized with original value
        
        # Convert row to dict and format time based on timezone
        event_dict = {
            'date': row['date'].strftime("%Y-%m-%d") if hasattr(row['date'], 'strftime') else str(row['date']),
            'time': converted_time,
            'event': row['event'],
            'impact': row['impact'],
            'forecast': row['forecast'] if 'forecast' in row else '',
            'previous': row['previous'] if 'previous' in row else ''
        }
        
        # Add to events list
        events.append(event_dict)
    
    # Debug the first processed event
    if events:
        print(f"First processed event: {events[0]}")
        
    print(f"Returning {len(events)} events")
    return events

@anvil.server.callable
def get_next_high_impact_event(target_timezone="UTC"):
    """
    Get the next upcoming high impact event from the current time
    
    Args:
        target_timezone (str, optional): Target timezone. Defaults to "UTC".
        
    Returns:
        dict: Event dictionary with date, time, event, impact details or None if no events found
    """
    import datetime
    import pytz
    from anvil.tables import app_tables
    
    print(f"Fetching next high impact event, timezone: {target_timezone}")
    
    # Get timezone object
    if target_timezone == "Eastern":
        tz = pytz.timezone("America/New_York")
    elif target_timezone == "Central":
        tz = pytz.timezone("America/Chicago")
    elif target_timezone == "Mountain":
        tz = pytz.timezone("America/Denver")
    elif target_timezone == "Pacific":
        tz = pytz.timezone("America/Los_Angeles")
    else:
        tz = pytz.timezone("UTC")
    
    # Get current time in the target timezone
    now = datetime.datetime.now(tz)
    current_date = now.date()
    
    # Get all rows from marketcalendar table
    rows = app_tables.marketcalendar.search()
    
    # Filter for high impact events on or after current date
    high_impact_events = []
    for row in rows:
        row_date = row['date']
        
        # Convert string date to date object if needed
        if isinstance(row_date, str):
            try:
                row_date = datetime.datetime.strptime(row_date, "%Y-%m-%d").date()
            except:
                # Skip if conversion fails
                continue
        
        # Skip past events
        if row_date < current_date:
            continue
        
        # Check for high impact
        impact = row.get('impact', '')
        if isinstance(impact, str) and impact.lower() == 'high':
            # This is a high impact event on or after current date
            # Parse the time and create a full datetime
            time_str = row.get('time', '')
            if time_str and isinstance(time_str, str):
                try:
                    # Try to parse the time (12-hour format with AM/PM)
                    try:
                        event_time = datetime.datetime.strptime(time_str, "%I:%M %p").time()
                    except ValueError:
                        # Try 24-hour format
                        event_time = datetime.datetime.strptime(time_str, "%H:%M").time()
                    
                    # Create full datetime in UTC
                    event_datetime = datetime.datetime.combine(row_date, event_time)
                    
                    # Make timezone aware (assume stored in UTC)
                    event_datetime = pytz.UTC.localize(event_datetime)
                    
                    # If it's today, check if it's in the future
                    if row_date == current_date and event_datetime.astimezone(tz) <= now:
                        # Event has already passed today
                        continue
                    
                    # Convert to target timezone for display
                    converted_dt = event_datetime.astimezone(tz)
                    converted_time = converted_dt.strftime("%I:%M %p")
                    
                    # Create event dict with datetime for sorting
                    event_dict = {
                        'date': row_date.strftime("%Y-%m-%d") if hasattr(row_date, 'strftime') else str(row_date),
                        'time': converted_time,
                        'event': row['event'],
                        'impact': impact,
                        'datetime': event_datetime,  # For sorting
                        'datetime_tz': converted_dt  # For countdown calc
                    }
                    high_impact_events.append(event_dict)
                except Exception as e:
                    print(f"Error parsing event time: {e}")
                    # Include the event anyway with the original time
                    event_dict = {
                        'date': row_date.strftime("%Y-%m-%d") if hasattr(row_date, 'strftime') else str(row_date),
                        'time': time_str,
                        'event': row['event'],
                        'impact': impact,
                        # Use current datetime plus 1 year as fallback for sorting
                        # This ensures events with unparseable times sort after events with valid times
                        'datetime': datetime.datetime.now(pytz.UTC) + datetime.timedelta(days=365),
                        'datetime_tz': now + datetime.timedelta(days=365)
                    }
                    high_impact_events.append(event_dict)
    
    # If no events found, return None
    if not high_impact_events:
        print("No upcoming high impact events found")
        return None
    
    # Sort by datetime (earliest first)
    high_impact_events.sort(key=lambda x: x['datetime'])
    
    # Get the next event
    next_event = high_impact_events[0]
    
    # Remove the datetime keys used for sorting before returning
    next_event_clean = {k: v for k, v in next_event.items() if k not in ['datetime']}
    
    print(f"Next high impact event: {next_event_clean['event']} on {next_event_clean['date']} at {next_event_clean['time']}")
    return next_event_clean

@anvil.server.callable
def debug_market_calendar_table():
    """Debug function to check the market calendar table structure and permissions"""
    try:
        # Check if we can access the table at all
        print("Attempting to access marketcalendar table...")
        
        # Try to get the number of rows
        try:
            row_count = len(list(app_tables.marketcalendar.search()))
            print(f"Successfully counted {row_count} rows in marketcalendar table")
        except Exception as e:
            print(f"Error counting rows: {e}")
        
        # Try to get a sample row
        try:
            rows = list(app_tables.marketcalendar.search())
            if len(rows) == 0:
                print("No rows found in marketcalendar table")
            elif len(rows) > 1:
                print("Warning: More than one row matched the query; returning the first row as sample.")
                sample_row = rows[0]
                print(f"Successfully got a sample row with keys: {list(sample_row.keys())}")
                print(f"Sample row values: {dict(sample_row)}")
            else:
                sample_row = rows[0]
                print(f"Successfully got a sample row with keys: {list(sample_row.keys())}")
                print(f"Sample row values: {dict(sample_row)}")
        except Exception as e:
            print(f"Error getting sample row: {e}")
        
        # Try to get table schema
        try:
            # This approach works in newer Anvil versions
            schema = app_tables.marketcalendar.list_columns()
            print(f"Table schema: {schema}")
        except Exception as e:
            print(f"Error getting table schema: {e}")
        
        return "Debugging completed - check server logs"
    except Exception as e:
        print(f"Overall debug error: {e}")
        import traceback
        print(traceback.format_exc())
        return f"Error: {e}"

@anvil.server.callable
def populate_sample_market_events():
    """
    Add sample market events to the marketcalendar table for testing purposes
    """
    try:
        import datetime
        
        # Check if the table already has events
        event_count = len(list(app_tables.marketcalendar.search()))
        if event_count > 0:
            print(f"Table already has {event_count} events, not adding samples")
            return f"Table already has {event_count} events"
        
        # Create sample events for the current date range
        today = datetime.date.today()
        
        # Add events for today through next month
        sample_events = [
            # Today's events
            {
                'date': today,
                'time': '08:30 AM',
                'event': 'Initial Jobless Claims',
                'impact': 'Medium',
                'forecast': '215K',
                'previous': '217K'
            },
            {
                'date': today,
                'time': '10:00 AM',
                'event': 'Existing Home Sales',
                'impact': 'Medium',
                'forecast': '4.20M',
                'previous': '4.38M'
            },
            
            # Tomorrow's events
            {
                'date': today + datetime.timedelta(days=1),
                'time': '09:45 AM',
                'event': 'Manufacturing PMI',
                'impact': 'High',
                'forecast': '51.8',
                'previous': '52.2'
            },
            
            # This week's events (adding a few days ahead)
            {
                'date': today + datetime.timedelta(days=3),
                'time': '02:00 PM',
                'event': 'Fed Interest Rate Decision',
                'impact': 'High',
                'forecast': '5.50%',
                'previous': '5.50%'
            },
            
            # Next week's events
            {
                'date': today + datetime.timedelta(days=7),
                'time': '08:30 AM',
                'event': 'Durable Goods Orders',
                'impact': 'Medium',
                'forecast': '0.5%',
                'previous': '0.2%'
            },
            
            # This month's events
            {
                'date': today + datetime.timedelta(days=12),
                'time': '08:30 AM',
                'event': 'Nonfarm Payrolls',
                'impact': 'High',
                'forecast': '180K',
                'previous': '175K'
            },
            
            # Next month's events
            {
                'date': today + datetime.timedelta(days=35),
                'time': '08:30 AM',
                'event': 'GDP',
                'impact': 'High',
                'forecast': '2.8%',
                'previous': '3.1%'
            }
        ]
        
        # Add the sample events to the database
        for event in sample_events:
            app_tables.marketcalendar.add_row(**event)
        
        print(f"Added {len(sample_events)} sample events to the marketcalendar table")
        return f"Added {len(sample_events)} sample events"
        
    except Exception as e:
        print(f"Error adding sample events: {e}")
        import traceback
        print(traceback.format_exc())
        return f"Error: {e}"
