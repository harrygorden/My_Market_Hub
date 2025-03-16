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
      
    # Set the items on the data grid
    self.set_data_grid_items(events)
    
    # Update UI to show status
    if len(events) == 0:
      print("No events found for the selected date range")
    else:
      print(f"Displaying {len(events)} events in the grid")
      
  def set_data_grid_items(self, events):
    """Helper method to set items on the data grid properly
    
    Args:
        events (list): List of event dictionaries
    """
    # Based on the YAML structure, we need to set items directly on the data_grid_market_events
    # The DataGrid will automatically forward these to its internal repeating panel
    
    # Convert events to the format expected by the row template
    processed_events = []
    for event in events:
      # Create dictionary with keys that EXACTLY match the data bindings in RowTemplate1
      # Important: The keys must match the data_key values in the DataGrid columns definition
      processed_event = {
        'date': str(event.get('date', '')),
        'time': str(event.get('time', '')),
        'event': str(event.get('event', '')),
        'impact': str(event.get('impact', '')),
        'forecast': str(event.get('forecast', '')),
        'previous': str(event.get('previous', ''))
      }
      processed_events.append(processed_event)
    
    print(f"Setting {len(processed_events)} events on the data grid")
    
    # For Anvil DataGrids, set items directly on the DataGrid component
    # This is the standard pattern and should work as long as the keys match the column data_keys
    try:
      # First, clear existing items
      self.data_grid_market_events.items = []
      
      # A small delay can help prevent rendering issues
      import time
      time.sleep(0.1)
      
      # Set the new items
      self.data_grid_market_events.items = processed_events
      
      # Debug what's now in the grid
      if hasattr(self.data_grid_market_events, 'items'):
        print(f"DataGrid now has {len(self.data_grid_market_events.items)} items")
        if len(self.data_grid_market_events.items) > 0:
          print(f"First event in grid: {self.data_grid_market_events.items[0]}")
    except Exception as e:
      print(f"Error setting items on DataGrid: {e}")
  
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
