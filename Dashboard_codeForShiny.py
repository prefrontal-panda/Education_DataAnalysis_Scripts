# This is to create an interactive dashboard using Shiny for Python

# Import requierd Libraries
import pandas as pd

# Shiny libraries
from shiny import reactive
from shiny.express import input, render, ui



# File Input Button
ui.input_file(id="f",label="Please upload your file:", 
              multiple=False,width=None, button_label='Browse...',
              placeholder='No file selected', capture=None)

# Sidebar
with ui.layout_sidebar():  
     with ui.sidebar(bg="#f8f8f8"):
         "Filtering Options"