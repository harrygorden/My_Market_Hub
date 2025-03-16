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
    Retrieve market calendar events for a specific date range and convert to specified timezone
    
    Args:
        start_date (datetime.date): Start date (inclusive)
        end_date (datetime.date): End date (inclusive)
        target_timezone (str): Target timezone (UTC, Eastern, Central, Mountain, Pacific)
        
    Returns:
        list: List of event dictionaries with times converted to target timezone
    """
    try:
        import pytz
        import datetime
        
        # Debugging the incoming parameters
        print(f"Type of start_date: {type(start_date)}")
        print(f"Value of start_date: {start_date}")
        print(f"Type of end_date: {type(end_date)}")
        print(f"Value of end_date: {end_date}")
        print(f"Target timezone: {target_timezone}")
        
        # Ensure dates are datetime.date objects
        # If they're strings (ISO format), convert them
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Define timezone mappings
        timezone_map = {
            "Eastern": "US/Eastern",
            "Central": "US/Central",
            "Mountain": "US/Mountain",
            "Pacific": "US/Pacific",
            "UTC": "UTC"
        }
        
        # Get the pytz timezone object
        tz = pytz.timezone(timezone_map[target_timezone])
        
        # Get all rows and filter manually - this is the most reliable approach
        try:
            all_rows = list(app_tables.marketcalendar.search())
            print(f"Retrieved {len(all_rows)} total rows from marketcalendar")
            
            # Filter for dates in the range - using dictionary access style with brackets
            events_list = []
            for row in all_rows:
                try:
                    row_date = row['date']  # Using dict-style access
                    
                    # Convert string date to datetime.date if needed
                    if isinstance(row_date, str):
                        try:
                            row_date = datetime.datetime.strptime(row_date, '%Y-%m-%d').date()
                        except ValueError:
                            # If format is not YYYY-MM-DD, try alternate formats
                            try:
                                row_date = datetime.datetime.strptime(row_date, '%m/%d/%Y').date()
                            except ValueError:
                                continue  # Skip this row if we can't parse the date
                    
                    # Check if row_date is within the specified date range
                    if isinstance(row_date, (datetime.date, datetime.datetime)) and start_date <= row_date <= end_date:
                        events_list.append(row)
                except Exception as e:
                    print(f"Error accessing row date: {e} - Row: {row}")
            
            print(f"Filtered to {len(events_list)} rows in date range {start_date} to {end_date}")
            
            # Print the first event to understand structure
            if events_list:
                row = events_list[0]
                print(f"First event in range: Date={row['date']}, Event={row['event']}")
                
        except Exception as e:
            print(f"Error retrieving/filtering events: {e}")
            import traceback
            print(traceback.format_exc())
            events_list = []
        
        # Convert to list of dictionaries with timezone conversion
        event_list = []
        for event_row in events_list:
            try:
                # Convert Anvil row to dictionary safely
                event = {}
                
                # Handle date
                try:
                    date_val = event_row['date']
                    if isinstance(date_val, (datetime.date, datetime.datetime)):
                        event['date'] = date_val.strftime('%Y-%m-%d')
                    else:
                        event['date'] = str(date_val)
                except:
                    event['date'] = ''
                
                # Handle other fields safely
                for field in ['time', 'event', 'impact', 'forecast', 'previous']:
                    try:
                        event[field] = str(event_row[field]) if event_row[field] is not None else ''
                    except:
                        event[field] = ''
                
                # Convert time if target timezone is not UTC
                if target_timezone != "UTC" and event['time'] and event['date']:
                    try:
                        # Parse the date
                        event_date = datetime.datetime.strptime(event['date'], '%Y-%m-%d').date()
                        
                        # Handle various time formats
                        time_str = event['time'].lower().replace('am', ' am').replace('pm', ' pm')
                        
                        # Try to parse the time string
                        try:
                            event_time = datetime.datetime.strptime(time_str, '%I:%M %p').time()
                        except ValueError:
                            try:
                                event_time = datetime.datetime.strptime(time_str, '%H:%M').time()
                            except ValueError:
                                # If we can't parse the time, skip conversion
                                event_list.append(event)
                                continue
                        
                        # Create a datetime object in UTC
                        utc_dt = datetime.datetime.combine(event_date, event_time, tzinfo=pytz.UTC)
                        
                        # Convert to target timezone
                        local_dt = utc_dt.astimezone(tz)
                        
                        # Format the time for display
                        event['time'] = local_dt.strftime('%I:%M %p')
                        
                    except Exception as e:
                        print(f"Error converting time: {e}")
                
                event_list.append(event)
                
            except Exception as e:
                print(f"Error processing event row: {e}")
        
        # Debug the final output list
        print(f"Returning {len(event_list)} events")
        if event_list:
            print(f"First processed event: {event_list[0]}")
        return event_list
        
    except Exception as e:
        print(f"Error retrieving market calendar events with timezone conversion: {e}")
        import traceback
        print(traceback.format_exc())
        return []

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
