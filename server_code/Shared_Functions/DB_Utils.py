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
def save_market_calendar_event(event_data):
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
            
    Returns:
        row: The newly created or updated table row
    """
    try:
        # Debug the incoming event data with special focus on the impact
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
                print(f"Updating impact from '{existing_event['impact']}' to '{event_data['impact']}'")
            
            if event_data.get('forecast') and event_data['forecast'] != existing_event['forecast']:
                updates['forecast'] = event_data['forecast']
                
            if event_data.get('previous') and event_data['previous'] != existing_event['previous']:
                updates['previous'] = event_data['previous']
                
            # Only update if we have changes
            if updates:
                existing_event.update(**updates)
                print(f"Updated existing event: {event_data['event']} on {event_data['date']} at {event_data['time']}")
                print(f"New impact value in database: '{existing_event['impact']}'")
            else:
                print(f"No changes needed for: {event_data['event']} on {event_data['date']} at {event_data['time']}")
                
            return existing_event
        else:
            # Create new event
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
            print(f"Added new event: {event_data['event']} on {event_data['date']} at {event_data['time']}")
            print(f"Impact value saved to database: '{new_event['impact']}'")
            return new_event
    
    except Exception as e:
        print(f"Error saving market calendar event: {e}")
        return None

@anvil.server.callable
def save_multiple_market_calendar_events(events_list):
    """
    Save multiple market calendar events to the marketcalendar Anvil table
    
    Args:
        events_list (list): List of event dictionaries
        
    Returns:
        dict: Statistics about processed events containing:
            - total: Total number of events processed
            - existing: Number of existing events (skipped or updated)
            - new: Number of newly added events
    """
    if not events_list:
        print("No events to save")
        return {"total": 0, "existing": 0, "new": 0}
    
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
        
        result = save_market_calendar_event(event)
        
        if result:
            if existing_event:
                stats["existing"] += 1
            else:
                stats["new"] += 1
    
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
