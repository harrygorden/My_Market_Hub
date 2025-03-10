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
#
# Here is an example - you can replace it with your own:
#
# @anvil.server.callable
# def say_hello(name):
#   print("Hello, " + name + "!")
#   return 42
#

@anvil.server.callable
def save_market_calendar_events(events, clear_existing=False):
    """
    Save a list of market calendar events to the marketcalendar Anvil table.
    
    Args:
        events (list): List of dictionaries containing event data
        clear_existing (bool, optional): If True, clears all existing events for dates in the provided events. Default is False.
    
    Returns:
        dict: Results containing counts of added, updated, and skipped events
    """
    if not events:
        print("No events to save")
        return {"added": 0, "updated": 0, "skipped": 0, "error": None}
    
    try:
        print("Attempting to access marketcalendar table")
        calendar_table = app_tables.marketcalendar
        
        # Track statistics
        results = {
            "added": 0,
            "updated": 0,
            "skipped": 0,
            "error": None
        }
        
        # Convert string dates to date objects for the events
        for event in events:
            if 'date' in event and isinstance(event['date'], str):
                try:
                    event['date'] = datetime.datetime.strptime(event['date'], "%Y-%m-%d").date()
                except ValueError as e:
                    print(f"Error converting date '{event['date']}': {e}")
                    # Try to handle other formats if needed
        
        # Get unique dates in the events list
        event_dates = set(event['date'] for event in events if 'date' in event)
        print(f"Found {len(event_dates)} unique dates in events")
        
        # Handle clear_existing option - remove existing events for the dates we're importing
        if clear_existing and event_dates:
            print(f"Clearing existing events for {len(event_dates)} dates")
            
            # Query existing rows for these dates and delete them
            rows_to_delete = calendar_table.search(date=q.any_of(*event_dates))
            delete_count = 0
            
            for row in rows_to_delete:
                row.delete()
                delete_count += 1
            
            print(f"Deleted {delete_count} existing events")
        
        # Get existing events to check for duplicates (if not clearing)
        existing_events = {}
        if not clear_existing:
            print("Fetching existing events from marketcalendar table")
            for row in calendar_table.search():
                # Create a unique key for each event based on date, time, and event name
                # Convert date to string format for key creation
                date_str = row['date'].strftime("%Y-%m-%d") if isinstance(row['date'], datetime.date) else str(row['date'])
                key = f"{date_str}_{row['time']}_{row['event']}"
                existing_events[key] = row
            
            print(f"Found {len(existing_events)} existing events in the table")
        
        # Add or update events
        for event in events:
            try:
                # Ensure required fields exist
                required_fields = ['date', 'event']
                if not all(field in event for field in required_fields):
                    print(f"Skipping event, missing required fields: {event}")
                    results["skipped"] += 1
                    continue
                
                # Make a copy of the event for database operation
                event_for_db = event.copy()
                
                # Ensure date is a datetime.date object
                if not isinstance(event_for_db['date'], datetime.date):
                    try:
                        event_for_db['date'] = datetime.datetime.strptime(event_for_db['date'], "%Y-%m-%d").date()
                    except ValueError as e:
                        print(f"Invalid date format in event: {event_for_db['date']}, error: {e}")
                        results["skipped"] += 1
                        continue
                
                # Add default values for fields that might be missing
                if 'time' not in event_for_db:
                    event_for_db['time'] = ''
                if 'currency' not in event_for_db:
                    event_for_db['currency'] = ''
                if 'impact' not in event_for_db:
                    event_for_db['impact'] = ''
                if 'forecast' not in event_for_db:
                    event_for_db['forecast'] = ''
                if 'previous' not in event_for_db:
                    event_for_db['previous'] = ''
                
                # Check for duplicates if not clearing existing events
                if not clear_existing:
                    # Create a unique key for this event
                    # First ensure we have a string format of the date for the key
                    date_str = event_for_db['date'].strftime("%Y-%m-%d") if isinstance(event_for_db['date'], datetime.date) else str(event_for_db['date'])
                    key = f"{date_str}_{event_for_db.get('time', '')}_{event_for_db['event']}"
                    
                    # Check if this event already exists
                    if key in existing_events:
                        # Event exists - update it if needed
                        existing_row = existing_events[key]
                        
                        # Check if any fields need updating
                        needs_update = False
                        for field in ['impact', 'forecast', 'previous']:
                            if field in event_for_db and existing_row[field] != event_for_db[field]:
                                needs_update = True
                                break
                        
                        # Update if needed
                        if needs_update:
                            print(f"Updating existing event: {event_for_db['event']} on {date_str}")
                            # Update only fields that might change
                            existing_row['impact'] = event_for_db['impact']
                            existing_row['forecast'] = event_for_db['forecast']
                            existing_row['previous'] = event_for_db['previous']
                            results["updated"] += 1
                        else:
                            # Skip if no update needed
                            results["skipped"] += 1
                        continue
                
                # New event - add it
                date_str = event_for_db['date'].strftime("%Y-%m-%d") if isinstance(event_for_db['date'], datetime.date) else str(event_for_db['date'])
                print(f"Adding new event: {event_for_db['event']} on {date_str}")
                calendar_table.add_row(
                    date=event_for_db['date'],
                    time=event_for_db['time'],
                    event=event_for_db['event'],
                    currency=event_for_db['currency'],
                    impact=event_for_db['impact'],
                    forecast=event_for_db['forecast'],
                    previous=event_for_db['previous']
                )
                results["added"] += 1
                
            except Exception as e:
                print(f"Error processing event {event.get('event', '')}: {e}")
                results["skipped"] += 1
        
        print(f"Calendar update complete: {results['added']} added, {results['updated']} updated, {results['skipped']} skipped")
        return results
        
    except Exception as e:
        error_message = f"Error saving events to table: {str(e)}"
        print(error_message)
        # Print table information for debugging
        try:
            print(f"Exception type: {type(e).__name__}")
            print("Trying to list available tables:")
            for table_name in dir(app_tables):
                if not table_name.startswith('__'):
                    print(f" - {table_name}")
        except Exception as inner_e:
            print(f"Error while debugging table access: {inner_e}")
        
        return {"added": 0, "updated": 0, "skipped": 0, "error": error_message}

@anvil.server.callable
def clear_market_calendar_events(start_date=None, end_date=None):
    """
    Clear market calendar events from the marketcalendar table.
    
    Args:
        start_date (str or datetime.date, optional): Start date (inclusive) for events to clear. If None, no start date filter.
        end_date (str or datetime.date, optional): End date (inclusive) for events to clear. If None, no end date filter.
    
    Returns:
        int: Number of events deleted
    """
    try:
        print("Attempting to clear market calendar events")
        calendar_table = app_tables.marketcalendar
        
        # Convert string dates to date objects if needed
        if start_date and isinstance(start_date, str):
            try:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid start date format: {start_date}")
                return 0
                
        if end_date and isinstance(end_date, str):
            try:
                end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid end date format: {end_date}")
                return 0
        
        # Build the query based on provided date range
        if start_date and end_date:
            print(f"Clearing events from {start_date} to {end_date}")
            rows = calendar_table.search(
                q.all_of(
                    q.greater_than_or_equal_to("date", start_date),
                    q.less_than_or_equal_to("date", end_date)
                )
            )
        elif start_date:
            print(f"Clearing events from {start_date} onwards")
            rows = calendar_table.search(q.greater_than_or_equal_to("date", start_date))
        elif end_date:
            print(f"Clearing events up to {end_date}")
            rows = calendar_table.search(q.less_than_or_equal_to("date", end_date))
        else:
            print("Clearing all events")
            rows = calendar_table.search()
        
        # Delete the rows
        count = 0
        for row in rows:
            row.delete()
            count += 1
        
        print(f"Deleted {count} events")
        return count
        
    except Exception as e:
        print(f"Error clearing market calendar events: {e}")
        return 0

@anvil.server.callable
def get_market_calendar_events(start_date=None, end_date=None, currencies=None):
    """
    Get market calendar events from the marketcalendar table.
    
    Args:
        start_date (str or datetime.date, optional): Start date (inclusive) for events to return. If None, no start date filter.
        end_date (str or datetime.date, optional): End date (inclusive) for events to return. If None, no end date filter.
        currencies (list, optional): List of currency codes to filter by. If None, all currencies are returned.
    
    Returns:
        list: List of event dictionaries
    """
    try:
        print("Retrieving market calendar events")
        calendar_table = app_tables.marketcalendar
        
        # Convert string dates to date objects if needed
        if start_date and isinstance(start_date, str):
            try:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid start date format: {start_date}")
                return []
                
        if end_date and isinstance(end_date, str):
            try:
                end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid end date format: {end_date}")
                return []
        
        # Build the query based on provided filters
        query_parts = []
        
        # Date range filters
        if start_date:
            query_parts.append(q.greater_than_or_equal_to("date", start_date))
        if end_date:
            query_parts.append(q.less_than_or_equal_to("date", end_date))
        
        # Currency filter
        if currencies:
            query_parts.append(q.any_of(*[q.equal_to("currency", curr) for curr in currencies]))
        
        # Execute the query with all filters
        if query_parts:
            if len(query_parts) == 1:
                rows = calendar_table.search(query_parts[0], tables.order_by("date", "time"))
            else:
                rows = calendar_table.search(q.all_of(*query_parts), tables.order_by("date", "time"))
        else:
            rows = calendar_table.search(tables.order_by("date", "time"))
        
        # Convert to list of dictionaries
        events = []
        for row in rows:
            # Convert date to string format for the returned dictionary
            date_str = row["date"].strftime("%Y-%m-%d") if isinstance(row["date"], datetime.date) else str(row["date"])
            
            event = {
                "date": date_str,
                "time": row["time"],
                "event": row["event"],
                "currency": row["currency"],
                "impact": row["impact"],
                "forecast": row["forecast"],
                "previous": row["previous"]
            }
            events.append(event)
        
        print(f"Retrieved {len(events)} events")
        return events
        
    except Exception as e:
        print(f"Error retrieving market calendar events: {e}")
        return []
