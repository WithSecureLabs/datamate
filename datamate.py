
from dask.distributed import Client
from utils import Utils
from panel.template import DefaultTheme
from bokeh.models import HoverTool
import dask.dataframe as dd
import holoviews as hv
import networkx as nx
import socket
import hvplot.networkx as hvnx
import argparse
import json
import pandas as pd
import os
import panel as pn
import random

def read_parquet(file_path, targeted_cols):
        
    #Initiate Dask Clientlocally
    # client = Client(n_workers=os.cpu_count())

    with Client(n_workers=os.cpu_count()) as client:
    
        # Read data from Parquet
        print("Read data from Parquet file")
        pdf = dd.read_parquet(file_path, columns=targeted_cols, engine="pyarrow")

        # Replace NaN with zeroes to be able to convert the columns to int
        print("Clean data")
        pdf['_BytesSent'] = pdf['_BytesSent'].fillna(0)
        pdf['_BytesReceived'] = pdf['_BytesReceived'].fillna(0)

        # Convert _Bytes, _BytesSent and _BytesReceived from float to int
        pdf['_BytesSent'] = pdf['_BytesSent'].astype('int64')
        pdf['_BytesReceived'] = pdf['_BytesReceived'].astype('int64')

        # Drop rows with empty IPs (SourceIP/DestinationIP)
        pdf = pdf[~pdf[['_SourceIP', '_DestinationIP']].isnull().any(axis=1)]

        # Convert _DestinationPort to int
        pdf['_DestinationPort'] = pdf['_DestinationPort'].astype('int64')

        # Convert timestamp to datetime
        
        pdf['_@timestamp'] = dd.to_datetime(pdf['_@timestamp'].str[:19])

        # Set timestamp as index
        pdf = pdf.set_index('_@timestamp')

        print("Make data persistent")
        pdf = pdf.persist()

        print("Compute data")
        pdf = pdf.compute()
        print("Return data")
        return pdf

class Datamate:

    def __init__(self, datafile, config) -> None:
        self.datafile = datafile
        self.targeted_cols = config['targeted_cols']
        self.known_servers = config['known_servers']
        self.df = None
        self.table = None        
        self.start_date = None
        self.end_date = None
        self.max_bytes_sent = 0
        self.max_bytes_received = 0
        self.bytes_sent_widget = None
        self.bytes_received_widget = None
        self.start_date_widget = None
        self.end_date_widget = None
        self.page_size_selector = None
        self.legends = pn.Row()
        self.filter_ip_input = None
        self.graph = pn.Column(
            pn.pane.HTML("<div style='align-items:center; justify-content: center;'>Graph will be displayed here</div>", width=600, height=600,)
        )

    @staticmethod
    def classify_ip(ip_to_check: str):
        try:
            packed_ip = socket.inet_aton(str(ip_to_check))
            ip = socket.inet_ntoa(packed_ip)
            if ip.startswith('10.') or ip.startswith('172.') or ip.startswith('192.168.'):
                return 'Internal'
            else:
                return 'External'
        except socket.error:
            return 'Invalid'

    def create_table(self):
        self.table = pn.widgets.Tabulator(
            self.df.head(50),
            show_index=True,
            pagination='local', 
            page_size=10,
            selectable=False,
            disabled=True,
            theme='bootstrap4'
        )

    def create_date_widget(self, start_date, end_date, value):
        return pn.widgets.DatePicker(
            value=value.date(),
            start=start_date.date(),
            end=end_date.date(),
            enabled_dates=[start_date.date()+pd.Timedelta(days=i) for i in range((end_date - start_date).days + 1)],
            width=100,
            name="Start date"
        )
    
    def create_range_slider(self, max_value, name):
         return pn.widgets.RangeSlider(
            value=(0, max_value),
            start=0,
            end=max_value,
            step=max_value//1000,
            name=name,
            bar_color="#5f9ed1",
            width=400
        )
    
    @staticmethod
    def get_blue_circle(self):
        """Function to create a blue circle element associated with External unknown IPs, used for graph custom legend"""
        return pn.pane.HTML(
            """
            <div style='background-color: blue; border-radius:50%; width:30px; height:30px; display:flex;align-items:center; justify-content: center; white-space: nowrap;'>
                <span style="padding-left: 160px">External unknown IP</span>
            </div>
            """,
            width=160
        )
    
    @staticmethod
    def get_green_circle(self):
        """Function to create a green circle element associated with Internal unknown IPs, used for graph custom legend"""
        return pn.pane.HTML(
            """
            <div style='background-color: green; border-radius:50%; width:30px; height:30px; display:flex;align-items:center; justify-content: center; white-space: nowrap;'>
                <span style="padding-left: 160px">Internal unknown IP</span>
            </div>
            """,
            width=160
        )
    
    @staticmethod
    def get_blue_square(self):
        """Function to create a blue square element associated with External filtered IPs, used for graph custom legend"""
        return pn.pane.HTML(
            """
            <div style='background-color: blue; width:30px; height:30px; display:flex;align-items:center; justify-content: center; white-space: nowrap;'>
                <span style="padding-left: 160px">External filtered IP</span>
            </div>
            """,
            width=160
        )
    
    @staticmethod
    def get_green_square(self):
        """Function to create a green square element associated with Internal filtered IPs, used for graph custom legend"""
        return pn.pane.HTML(
            """
            <div style='background-color: green; width:30px; height:30px; display:flex;align-items:center; justify-content: center; white-space: nowrap;'>
                <span style="padding-left: 160px">Internal filtered IP</span>
            </div>
            """,
            width=160
        )
    
    @staticmethod
    def get_green_diamond(self):
        """Function to create a green diamond element associated with Internal MAIL IPs, used for graph custom legend"""
        return pn.pane.HTML(
            """
            <div style='background-color: green; width:23px; height:23px; display:flex;align-items:center; justify-content: center; white-space: nowrap; transform:rotate(45deg);'>
                <span style="padding-left: 160px; transform:rotate(-45deg);">Internal MAIL IP</span>
            </div>
            """,
            style={'padding-top': '5px'},
            width=160
        )
    
    @staticmethod
    def get_green_triangle(self):
        """Function to create a green triangle element associated with Internal DB IPs, used for graph custom legend"""
        return pn.pane.HTML(
            """
            <div style='width:0px; height:0px; border-left: 15px solid transparent; border-right: 15px solid transparent; border-bottom: 30px solid green; display:flex;align-items:center; justify-content: center; white-space: nowrap;'>
                <span style="padding-top: 30px; padding-left: 160px; color: black;">Internal DB IP</span>
            </div>
            """,
            width=160
        )
    
    def update_graph(self, event=None):
        """Function to create/update the graph based on filtered IP(s)"""

        # Make a list of IPs from the input value
        ip_filters = [ip.strip() for ip in self.filter_ip_input.value.split(',')]

        # Extract the data containing any of the filtered IP(S)
        graph_df = self.df[self.df['_SourceIP'].isin(ip_filters) | self.df['_DestinationIP'].isin(ip_filters)]

        random.seed(100)

        max_bytes_sent = graph_df['_BytesSent'].max()
        max_bytes_received = graph_df['_BytesReceived'].max()

        # Separate target, source and calculate weight based on total bytes exchanged
        # TODO: Improve weight calculation 
        data = {
            'target': graph_df['_DestinationIP'],
            'source': graph_df['_SourceIP'],
            'weight': (graph_df['_BytesSent'] + graph_df['_BytesReceived']) / (max_bytes_sent + max_bytes_received)
        }

        data_df = pd.DataFrame(data=data)

        # Get frequency of all IPs, from target and source altogether to define the size of the node
        stacked_df = data_df[['target', 'source']].stack().value_counts().reset_index().rename(columns={'index': 'label', 0: 'connections'})

        # Define the Graph
        G = nx.Graph()

        # We need max connections, to enable weighted connections 
        max_connections = stacked_df['connections'].max()

        # Add nodes
        for i, row in stacked_df.iterrows():
            # If the label is the filtered IP, in this case ip, the shape will be a square, else a circle
            cust_shape = 'square' if row['label'] in ip_filters else 'circle'
            cust_color = 'green' if self.classify_ip(row['label']) == 'Internal' else 'blue'

            if row['label'] in self.known_servers.keys():
                if 'MAIL' in self.known_servers[row['label']].upper():
                    cust_shape = 'diamond'
                    cust_color = 'green'
                elif 'DB' in self.known_servers[row['label']].upper():
                    cust_shape = 'triangle'
                    cust_color = 'green'
            
            G.add_node(
                row['label'], 
                connections=row['connections'],
                size=10+row['connections']/max_connections,
                color=cust_color,
                shape=cust_shape,
                network_role=self.known_servers[row['label']] if row['label'] in self.known_servers.keys() else 'Unknown IP'
            )

        # Add edges
        for i, row in data_df.iterrows():
            G.add_edge(
                row['target'], 
                row['source'], 
                weight=row['weight'],
                edge_style='solid' if row['weight'] > 0.3 else 'dashed'
            )


        # Define tools
        tools = HoverTool(
            tooltips=[
                ('IP', '@index_hover'),
                ('Connections', '@connections'),
                ('Role', '@network_role')
            ]
        )

        # Draw the graph
        filtered_graph = hvnx.draw(G).opts(
            node_marker='shape', 
            node_color='color', 
            node_size='size', 
            width=600, 
            height=600, 
            directed=True, 
            edge_line_dash='edge_style', 
            edge_color='orange', 
            tools=[tools]
                        
        )
        # Update the graph objects and set the custom legend
        self.graph.objects = [filtered_graph]
        self.set_custom_legend()        

    def set_custom_legend(self):
        """Function to set the custom legend regarding the graph nodes"""
        # Get the symbols
        blue_circle = self.get_blue_circle()
        green_circle = self.get_green_circle()
        blue_square = self.get_blue_square()
        green_square = self.get_green_square()
        green_diamond = self.get_green_diamond()
        green_triangle = self.get_green_triangle()

        # Set the custom legend
        self.legends.objects = [pn.Row(
                blue_circle,
                green_circle,
                blue_square,
                green_square,
                green_diamond,
                green_triangle
            )
        ]
    
    def run(self):
        """Function to launch the dashboard"""
        # Read the Parquet file
        self.df = read_parquet(self.datafile, self.targeted_cols)

        # Get useful info from the dataframe using Utils 
        utils = Utils()
        self.max_bytes_sent = utils.get_max_bytes_sent(self.df)
        self.max_bytes_received = utils.get_max_bytes_received(self.df)
        self.start_date = utils.get_start_date(self.df)
        self.end_date = utils.get_end_date(self.df)

        # Update widgets elements
        self.start_date_widget = self.create_date_widget(self.start_date, self.end_date, self.start_date)
        self.end_date_widget = self.create_date_widget(self.start_date, self.end_date, self.end_date)
        self.bytes_sent_widget = self.create_range_slider(self.max_bytes_sent, "Bytes sent")
        self.bytes_received_widget = self.create_range_slider(self.max_bytes_received, "Bytes received")
        self.page_size_selector = pn.widgets.Select(name='Page size', options=[10, 15, 20, 25, 30], width=55)
        update_button = pn.widgets.Button(name="Update", align="end", button_type="success", width=100)
        reload_button = pn.widgets.Button(name="Reload", align="end", button_type="success", width=100)
        reset_filters_button = pn.widgets.Button(name="↺", align="end", button_type="success", width=32)        

        # TODO: Implement "select_data" feature
        # select_data_button, load_data_button and cancel_button are defined but not in use
        # To be used within the feature "select_data" which should also consider different config given a selected parquet file
        # select_data_button should open a template.modal (popup)
        # select_data_button = pn.widgets.Button(name="Select data", align="end", button_type="success", width=100)
        # load_data_button = pn.widgets.Button(name="Load data", align="end", button_type="success", width=100)
        # cancel_button = pn.widgets.Button(name="Cancel", align="end", button_type="success", width=100)
        # FileSelector can be used: data_selector = pn.widgets.FileSelector(os.path.abspath(os.sep))

        # Group buttons
        buttons_widget = pn.Row(
            update_button,
            reload_button,
            reset_filters_button,
            self.page_size_selector
        )

        # Group filters
        filters_widget = pn.Row(
            self.start_date_widget,
            self.end_date_widget,
            pn.Column(
                self.bytes_sent_widget,
                self.bytes_received_widget
            )
        )

        # Create the table element
        self.create_table()

        # Bind buttons to functions
        pn.bind(self.filter_data, update_button, watch=True)
        pn.bind(self.reset_filters, reset_filters_button, watch=True)
        pn.bind(self.reload_data, reload_button, watch=True)
        pn.bind(self.set_page_size, self.page_size_selector, watch=True)

        # Define first tab of the dashboard
        data_exploration_tab = pn.Column(
            filters_widget,
            buttons_widget,
            self.table
        )

        # Create widgets for second tab
        data_visualization_header = pn.pane.HTML("<div>Visualize network connections as a graph, based on specific IP(s).</div>")
        self.filter_ip_input = pn.widgets.TextInput(name='Filter IP(s)', placeholder='Add IP(s) here...')
        update_graph_button = pn.widgets.Button(name="Visualize graph", align="end", button_type="success", width=150)
        

        # Bind buttons to functions
        pn.bind(self.update_graph, update_graph_button, watch=True)


        # Define second tab of the dashboard
        data_visualization_tab = pn.Column(
            data_visualization_header,
            pn.Row(self.filter_ip_input, update_graph_button),
            self.legends,
            self.graph
        )

        # Setup the main layout
        main = pn.Tabs(
            ("🔢 Data exploration", data_exploration_tab),
            ('📈 Data visualization', data_visualization_tab)
        )
        self.template(main)

    def reload_data(self, event=None) -> None:
        self.table.value = self.df.head(50)
        self.reset_filters()
    
    def set_page_size(self, event=None) -> None:
        """Function to change the page_size of thetable"""
        self.table.page_size = self.page_size_selector.value
    
    def reset_filters(self, event=None) -> None:
        """Function to resetthe filters to the default values"""
        self.start_date_widget.value = self.start_date.date()
        self.end_date_widget.value = self.end_date.date()
        self.bytes_sent_widget.value = (0, self.max_bytes_sent)
        self.bytes_received_widget.value = (0, self.max_bytes_received)
        # show_index_checkbox.value = False 

    def filter_data(self, event=None) -> None:
        """Function to update the table based on the filters"""
        filtered_df = self.df

        if self.bytes_received_widget.value != (0, self.max_bytes_received):
            filtered_df = filtered_df[filtered_df['_BytesReceived'].between(*self.bytes_received_widget.value)]
        
        if self.bytes_sent_widget.value != (0, self.max_bytes_sent):
            filtered_df = filtered_df[filtered_df['_BytesSent'].between(*self.bytes_sent_widget.value)]

        if self.start_date_widget.value != self.start_date.date() or self.end_date_widget.value != self.end_date.date():
            filtered_df = filtered_df[self.start_date_widget.value:self.end_date_widget.value]

        self.table.value = filtered_df.head(50)


    def template(self, main):
        template = pn.template.MaterialTemplate(
            title="Datamate",
            theme_toggle = 'Off',
            header_background="#5f9ed1",
            main=[main]
        )
        pn.serve(template)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        help="path to config file",
        required=True,
    )
    parser.add_argument(
        "--datafile",
        help="path to the parquet file",
        required=False,
    )
    
    args = parser.parse_args()
    config = json.load(open(args.config))

    datamate = Datamate(
        datafile=args.datafile,
        config=config
    )
    datamate.run()
