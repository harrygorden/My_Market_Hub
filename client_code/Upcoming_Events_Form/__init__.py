from ._anvil_designer import Upcoming_Events_FormTemplate
from anvil import *
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.server
import datetime
import pytz


class Upcoming_Events_Form(Upcoming_Events_FormTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    
    # Initialize default values for dropdowns
    self.drop_down_time_range.selected_value = "Today"
    self.drop_down_time_zone.selected_value = "Eastern"
    
    # Load initial data
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
    
    # Get events from server for the specified date range
    events = anvil.server.call('get_market_calendar_events_for_date_range', start_date, end_date)
    
    # Convert time zones if needed
    selected_timezone = self.drop_down_time_zone.selected_value
    if selected_timezone != "UTC":
      events = self.convert_event_times(events, selected_timezone)
    
    # Set the data source for the grid
    self.data_grid_market_events.items = events
  
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
  
  def convert_event_times(self, events, target_timezone):
    """Convert event times from UTC to the selected timezone"""
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
    
    # Create a new list for converted events
    converted_events = []
    
    for event in events:
      # Create a copy of the event
      converted_event = event.copy()
      
      # Only convert if we have both date and time
      if event['time'] and event['date']:
        try:
          # Parse the date and time
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
              converted_events.append(converted_event)
              continue
          
          # Create a datetime object in UTC
          utc_dt = datetime.datetime.combine(event_date, event_time, tzinfo=pytz.UTC)
          
          # Convert to target timezone
          local_dt = utc_dt.astimezone(tz)
          
          # Format the time for display
          converted_event['time'] = local_dt.strftime('%I:%M %p')
          
        except Exception as e:
          # If any conversion error, keep the original time
          print(f"Error converting time: {e}")
      
      # Add the event to our list
      converted_events.append(converted_event)
    
    return converted_events
