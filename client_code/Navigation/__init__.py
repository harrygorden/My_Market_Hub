from ._anvil_designer import NavigationTemplate
from anvil import *
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.server

# Import all form modules
from ..Home_Form import Home_Form
from ..ES_Analysis_Form import ES_Analysis_Form
from ..Flow_Analysis_Form import Flow_Analysis_Form
from ..Upcoming_Events_Form import Upcoming_Events_Form
from ..Whale_Watching_Form import Whale_Watching_Form
from ..Key_Lines_Form import Key_Lines_Form


class Navigation(NavigationTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    # Load Home form by default
    self.content_panel.add_component(Home_Form())
    
  def navigation_button_home_click(self, **event_args):
    """Handler for Home button click"""
    # Clear the content panel and add the Home form
    self.content_panel.clear()
    self.content_panel.add_component(Home_Form())

  def navigation_button_es_analysis_click(self, **event_args):
    """Handler for ES Analysis button click"""
    # Clear the content panel and add the ES Analysis form
    self.content_panel.clear()
    self.content_panel.add_component(ES_Analysis_Form())
    
  def navigation_button_flow_analysis_click(self, **event_args):
    """Handler for Flow Analysis button click"""
    # Clear the content panel and add the Flow Analysis form
    self.content_panel.clear()
    self.content_panel.add_component(Flow_Analysis_Form())
    
  def navigation_button_upcoming_events_click(self, **event_args):
    """Handler for Upcoming Events button click"""
    # Clear the content panel and add the Upcoming Events form
    self.content_panel.clear()
    self.content_panel.add_component(Upcoming_Events_Form())
    
  def navigation_button_whale_watching_click(self, **event_args):
    """Handler for Whale Watching button click"""
    # Clear the content panel and add the Whale Watching form
    self.content_panel.clear()
    self.content_panel.add_component(Whale_Watching_Form())
    
  def navigation_button_key_lines_click(self, **event_args):
    """Handler for Key Lines button click"""
    # Clear the content panel and add the Key Lines form
    self.content_panel.clear()
    self.content_panel.add_component(Key_Lines_Form())
