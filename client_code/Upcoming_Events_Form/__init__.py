from ._anvil_designer import Upcoming_Events_FormTemplate
from anvil import *
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.server
import datetime


class Upcoming_Events_Form(Upcoming_Events_FormTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Set up initial dropdown values
    
    # Set up date range dropdown options
    date_ranges = [
      'Today',
      'Tomorrow',
      'This Week',
      'Next Week',
      'This Month',
      'Next Month'
    ]
    self.drop_down_time_range.items = date_ranges
    self.drop_down_time_range.selected_value = 'Today'
    
    # Set up timezone dropdown options
    timezones = [
      'UTC',
      'Eastern',
      'Central',
      'Mountain',
      'Pacific'
    ]
    self.drop_down_time_zone.items = timezones
    self.drop_down_time_zone.selected_value = 'Eastern'
    
    # Debug the database
    print("Debugging the market calendar table...")
    debug_result = anvil.server.call('debug_market_calendar_table')
    print(f"Debug result: {debug_result}")
    
    # Try to populate sample data if needed
    print("Checking if we need to populate sample data...")
    sample_result = anvil.server.call('populate_sample_market_events')
    print(f"Sample data result: {sample_result}")
    
    # Refresh events
    self.refresh_events()

  def drop_down_time_zone_change(self, **event_args):
    """This method is called when the time zone is changed"""
    self.refresh_events()
    
  def drop_down_time_range_change(self, **event_args):
    """This method is called when the date range is changed"""
    self.refresh_events()
  
  def refresh_events(self):
    """Refresh the events grid based on selected filters"""
    # Get the date range based on selection
    start_date, end_date = self.get_date_range()
    
    # Convert dates to string format for passing to server
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Show loading in the UI
    print(f"Fetching events from {start_date_str} to {end_date_str} in {self.drop_down_time_zone.selected_value} timezone...")
    
    # Get events from server with timezone conversion
    selected_timezone = self.drop_down_time_zone.selected_value
    events = anvil.server.call('get_market_calendar_events_with_timezone', 
                              start_date_str, 
                              end_date_str, 
                              selected_timezone)
    
    # Debug client-side: show how many events we received
    print(f"Client received {len(events)} events")
    if len(events) > 0:
      print(f"First event: {events[0]}")
    
    # Clear existing items first
    self.data_grid_market_events.items = []
    
    # Add a small delay before setting the items (helps with rendering in some cases)
    import time
    time.sleep(0.1)
    
    # Set the items directly on the data grid
    # Make sure items are a list of dictionaries with matching keys to the columns
    processed_events = []
    for event in events:
      # Ensure all required keys are present
      processed_event = {
        'date': event.get('date', ''),
        'time': event.get('time', ''),
        'event': event.get('event', ''),
        'impact': event.get('impact', ''),
        'forecast': event.get('forecast', ''),
        'previous': event.get('previous', '')
      }
      processed_events.append(processed_event)
    
    # Set items on the data grid
    self.data_grid_market_events.items = processed_events
    
    # No need to manually refresh the repeating panel - it's handled internally by the data grid
    
    # Update UI to show status
    if len(events) == 0:
      print("No events found for the selected date range")
    else:
      print(f"Displaying {len(events)} events in the grid")
  
  def get_date_range(self):
    """Calculate start and end dates based on the selected range"""
    today = datetime.date.today()
    selected_range = self.drop_down_time_range.selected_value
    
    if selected_range == "Today":
      return today, today
    
    elif selected_range == "Tomorrow":
      tomorrow = today + datetime.timedelta(days=1)
      return tomorrow, tomorrow
    
    elif selected_range == "This Week":
      # Start of week (Monday)
      start_of_week = today - datetime.timedelta(days=today.weekday())
      # End of week (Sunday)
      end_of_week = start_of_week + datetime.timedelta(days=6)
      return start_of_week, end_of_week
    
    elif selected_range == "Next Week":
      # Start of next week (next Monday)
      start_of_next_week = today + datetime.timedelta(days=7-today.weekday())
      # End of next week (next Sunday)
      end_of_next_week = start_of_next_week + datetime.timedelta(days=6)
      return start_of_next_week, end_of_next_week
    
    elif selected_range == "This Month":
      # Start of month
      start_of_month = today.replace(day=1)
      # End of month (start of next month - 1 day)
      if today.month == 12:
        end_of_month = datetime.date(today.year + 1, 1, 1) - datetime.timedelta(days=1)
      else:
        end_of_month = datetime.date(today.year, today.month + 1, 1) - datetime.timedelta(days=1)
      return start_of_month, end_of_month
    
    elif selected_range == "Next Month":
      # Calculate next month
      next_month = today.month + 1
      next_month_year = today.year
      
      if next_month > 12:
        next_month = 1
        next_month_year += 1
      
      # Start of next month
      start_of_next_month = datetime.date(next_month_year, next_month, 1)
      
      # End of next month
      if next_month == 12:
        end_of_next_month = datetime.date(next_month_year + 1, 1, 1) - datetime.timedelta(days=1)
      else:
        end_of_next_month = datetime.date(next_month_year, next_month + 1, 1) - datetime.timedelta(days=1)
      
      return start_of_next_month, end_of_next_month
    
    # Default to today if something goes wrong
    return today, today
