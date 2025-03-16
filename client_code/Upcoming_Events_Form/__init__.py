from ._anvil_designer import Upcoming_Events_FormTemplate
from anvil import *
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.server
import datetime
import time


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
    
    # Initialize the countdown timer for high impact events
    self.next_high_impact_event = None
    self.update_high_impact_countdown()
    
    # Set up the existing timer component
    self.timer_1.interval = 1
    self.timer_1.set_event_handler('tick', self.update_countdown_display)
    
    # Start the timer (it will automatically start when interval > 0)
    
    # Refresh events
    self.refresh_events()

  def drop_down_time_zone_change(self, **event_args):
    """This method is called when the time zone is changed"""
    self.refresh_events()
    self.update_high_impact_countdown()
    
  def drop_down_time_range_change(self, **event_args):
    """This method is called when the date range is changed"""
    self.refresh_events()
    
  def check_box_low_change(self, **event_args):
    """This method is called when the Low impact checkbox is changed"""
    self.refresh_events()
    
  def check_box_medium_change(self, **event_args):
    """This method is called when the Medium impact checkbox is changed"""
    self.refresh_events()
    
  def check_box_high_change(self, **event_args):
    """This method is called when the High impact checkbox is changed"""
    self.refresh_events()
  
  def update_high_impact_countdown(self):
    """Fetch the next high impact event and prepare countdown data"""
    # Get the selected timezone
    selected_timezone = self.drop_down_time_zone.selected_value
    
    # Call server to get the next high impact event
    try:
      print(f"Fetching next high impact event, timezone: {selected_timezone}")
      self.next_high_impact_event = anvil.server.call('get_next_high_impact_event', selected_timezone)
      
      # If we have an event, update the countdown display immediately
      if self.next_high_impact_event:
        print(f"Successfully fetched event: {self.next_high_impact_event}")
        # Ensure the rich text box exists before attempting to update it
        if hasattr(self, 'rich_text_high_impact_event_countdown') and self.rich_text_high_impact_event_countdown:
          self.update_countdown_display()
        else:
          print("Error: rich_text_high_impact_event_countdown component not found")
      else:
        print("No upcoming high impact events found")
        if hasattr(self, 'rich_text_high_impact_event_countdown') and self.rich_text_high_impact_event_countdown:
          self.rich_text_high_impact_event_countdown.content = "No upcoming high impact events found."
    except Exception as e:
      print(f"Error fetching next high impact event: {type(e).__name__} - {str(e)}")
      if hasattr(self, 'rich_text_high_impact_event_countdown') and self.rich_text_high_impact_event_countdown:
        self.rich_text_high_impact_event_countdown.content = f"Error loading next high impact event: {type(e).__name__}"

  def update_countdown_display(self, **event_args):
    """Update the countdown display with current time remaining"""
    if not self.next_high_impact_event:
      try:
        # Try to refresh the event data if we don't have it
        self.update_high_impact_countdown()
        # If still no event after refresh, show message
        if not self.next_high_impact_event:
          self.rich_text_high_impact_event_countdown.content = "Next High Impact Event\n\nNo upcoming high impact events found."
      except Exception as e:
        print(f"Error refreshing countdown: {e}")
      return
    
    try:
      # Get current time
      now = datetime.datetime.now()
      
      # Parse event datetime
      event_date_str = self.next_high_impact_event.get('date', '')
      event_time_str = self.next_high_impact_event.get('time', '')
      event_name = self.next_high_impact_event.get('event', 'Unknown event')
      
      if not event_date_str or not event_time_str:
        self.rich_text_high_impact_event_countdown.content = (
          f"Next High Impact Event\n\n"
          f"{event_name}\n"
          f"(Missing date or time information)"
        )
        return
      
      # Try to parse datetime in different formats
      try:
        # Try 12-hour format first (8:30 AM)
        event_datetime = datetime.datetime.strptime(f"{event_date_str} {event_time_str}", "%Y-%m-%d %I:%M %p")
      except ValueError:
        try:
          # Try 24-hour format (08:30)
          event_datetime = datetime.datetime.strptime(f"{event_date_str} {event_time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
          # If all parsing fails, show error message
          self.rich_text_high_impact_event_countdown.content = (
            f"Next High Impact Event\n\n"
            f"{event_name} on {event_date_str} at {event_time_str}\n"
            f"(Unable to calculate countdown)"
          )
          return
    
      # Calculate time difference
      time_diff = event_datetime - now
      
      # Check if event is in the past
      if time_diff.total_seconds() <= 0:
        self.rich_text_high_impact_event_countdown.content = (
          f"Next High Impact Event\n\n"
          f"{event_name} at {event_time_str} has already occurred.\n"
          f"Please refresh to see the next upcoming high impact event."
        )
        # Update the next event (this will refresh at most once a minute to avoid server spam)
        if int(time.time()) % 60 == 0:
          self.update_high_impact_countdown()
        return
      
      # Calculate days, hours, minutes and seconds
      total_seconds = int(time_diff.total_seconds())
      days = total_seconds // 86400
      hours = (total_seconds % 86400) // 3600
      minutes = (total_seconds % 3600) // 60
      seconds = total_seconds % 60
      
      # Format countdown string
      if days > 0:
        countdown_text = f"{days}d {hours}h {minutes}m {seconds}s"
      elif hours > 0:
        countdown_text = f"{hours}h {minutes}m {seconds}s"
      elif minutes > 0:
        countdown_text = f"{minutes}m {seconds}s"
      else:
        countdown_text = f"{seconds}s"
      
      # Format the event day in a user-friendly way
      event_date = datetime.datetime.strptime(event_date_str, "%Y-%m-%d").date()
      today = now.date()
      tomorrow = today + datetime.timedelta(days=1)
      
      if event_date == today:
        friendly_day = "Today"
      elif event_date == tomorrow:
        friendly_day = "Tomorrow"
      else:
        # Get the day of week
        day_of_week = event_date.strftime("%A")
        
        # Calculate days difference
        days_diff = (event_date - today).days
        
        if days_diff < 7:
          friendly_day = f"This {day_of_week}"
        elif days_diff < 14:
          friendly_day = f"Next {day_of_week}"
        else:
          # For dates further away, use the month and day
          friendly_day = event_date.strftime("%B %d")
      
      # Update the rich text content with the new format
      self.rich_text_high_impact_event_countdown.content = (
        f"Next High Impact Event\n\n"
        f"{countdown_text}\n\n"
        f"{event_name} on {friendly_day} at {event_time_str}"
      )
    except Exception as e:
      print(f"Error updating countdown: {type(e).__name__} - {str(e)}")
      self.rich_text_high_impact_event_countdown.content = f"Next High Impact Event\n\nError updating countdown: {type(e).__name__}"
  
  def refresh_events(self):
    """Refresh the events grid based on selected filters"""
    # Get the date range based on selection
    start_date, end_date = self.get_date_range()
    
    # Convert dates to string format for passing to server
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Get events from server with timezone conversion
    selected_timezone = self.drop_down_time_zone.selected_value
    events = anvil.server.call('get_market_calendar_events_with_timezone', 
                              start_date_str, 
                              end_date_str, 
                              selected_timezone)
    
    # Convert to proper format for data grid
    processed_events = []
    
    # Get the current state of impact checkboxes
    show_low = self.check_box_low.checked
    show_medium = self.check_box_medium.checked
    show_high = self.check_box_high.checked
    
    # Process each event
    for event in events:
      # Create a new dictionary with the correct keys
      processed_event = {}
      
      # Ensure all fields are strings to avoid type issues
      processed_event['date'] = str(event.get('date', ''))
      processed_event['time'] = str(event.get('time', ''))
      processed_event['event'] = str(event.get('event', ''))
      processed_event['impact'] = str(event.get('impact', ''))
      processed_event['forecast'] = str(event.get('forecast', ''))
      processed_event['previous'] = str(event.get('previous', ''))
      
      # Apply impact filtering
      impact = processed_event['impact'].lower()
      
      # Only add events that match the selected impact levels
      if (impact == 'low' and show_low) or \
         (impact == 'medium' and show_medium) or \
         (impact == 'high' and show_high):
        # Add to our list
        processed_events.append(processed_event)
    
    # Set the items directly on the repeating panel inside the DataGrid
    # This is the correct way to populate a DataGrid in Anvil
    self.data_grid_repeating_panel.items = processed_events
    
    # Force UI refresh
    self.refresh_data_bindings()
    
    # Ensure the DataGrid is visible
    self.data_grid_market_events.visible = True

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
  
  def form_show(self, **event_args):
    """This method is called when the form is shown on the page"""
    # Make sure the timer is running
    if hasattr(self, 'timer_1') and self.timer_1:
      self.timer_1.interval = 1
  
  def form_hide(self, **event_args):
    """This method is called when the form is removed from the page"""
    # Stop the timer when the form is hidden
    if hasattr(self, 'timer_1') and self.timer_1:
      self.timer_1.interval = 0
