import os
import luigi
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import subprocess
import streamlit as st
from .config import DB_PATH, PROCESSED_DATA_PATH
from .store_data import StoreData

class VisualizeData(luigi.Task):
    """
    Task to create visualizations from stock data
    """
    symbol = luigi.Parameter()
    date = luigi.DateParameter()
    
    def requires(self):
        return StoreData(symbol=self.symbol, date=self.date)
    
    def output(self):
        # Create directory for visualizations
        viz_dir = f"{PROCESSED_DATA_PATH}/visualizations"
        os.makedirs(viz_dir, exist_ok=True)
        return luigi.LocalTarget(f"{viz_dir}/{self.symbol}_{self.date}_visualized.png")
    
    def run(self):
        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        
        # Query the data for the specific symbol
        query = f"SELECT * FROM stock_data WHERE Symbol = '{self.symbol}' ORDER BY Date"
        df = pd.read_sql_query(query, conn)
        
        # Convert Date to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Create a figure with multiple subplots
        fig, axs = plt.subplots(3, 1, figsize=(12, 15), sharex=True)
        
        # 1. Price chart with moving average
        axs[0].plot(df['Date'], df['Close'], label='Close Price')
        axs[0].plot(df['Date'], df['MA5'], label='5-Day MA', linestyle='--')
        axs[0].set_title(f'{self.symbol} Price and Moving Average')
        axs[0].set_ylabel('Price (USD)')
        axs[0].grid(True)
        axs[0].legend()
        
        # 2. Volume chart
        axs[1].bar(df['Date'], df['Volume'])
        axs[1].set_title(f'{self.symbol} Trading Volume')
        axs[1].set_ylabel('Volume')
        axs[1].grid(True)
        
        # 3. RSI chart
        axs[2].plot(df['Date'], df['RSI_14'])
        axs[2].axhline(y=70, color='r', linestyle='-', alpha=0.3)
        axs[2].axhline(y=30, color='g', linestyle='-', alpha=0.3)
        axs[2].set_title(f'{self.symbol} RSI (14)')
        axs[2].set_ylabel('RSI')
        axs[2].set_xlabel('Date')
        axs[2].grid(True)
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig(self.output().path)
        plt.close()
        
        # Close connection
        conn.close()
        
        print(f"Successfully created visualizations for {self.symbol}")

class StreamlitVisualization(luigi.Task):
    """
    Task to launch Streamlit for interactive visualization
    """
    def requires(self):
        # This task depends on all symbols being processed and stored in the database
        from .config import SYMBOLS
        from datetime import datetime
        
        return [VisualizeData(symbol=symbol, date=datetime.now().date()) for symbol in SYMBOLS]
    
    def output(self):
        # This is a marker file to indicate that streamlit app is ready
        return luigi.LocalTarget(f"{PROCESSED_DATA_PATH}/streamlit_ready.txt")
    
    def run(self):
        # Create the streamlit app file
        self._create_streamlit_app()
        
        # Create marker file
        with self.output().open('w') as f:
            f.write("Streamlit visualization ready")
        
        print("Streamlit app is ready to launch. Run 'streamlit run streamlit_app.py' to start the dashboard.")
    
    def _create_streamlit_app(self):
        """Creates the streamlit app file"""
        streamlit_code = """
import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# Import config to get DB path and symbols
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from luigi_pipeline.config import DB_PATH, SYMBOLS

# Set page configuration
st.set_page_config(
    page_title="Stock Market Dashboard",
    page_icon="📈",
    layout="wide"
)

# Title and description
st.title("📊 Stock Market Dashboard")
st.markdown("Interactive visualization of stock market data")

# Connect to database
@st.cache_resource
def get_connection():
    return sqlite3.connect(DB_PATH)

# Load data from database
@st.cache_data
def load_data(symbol, days=30):
    conn = get_connection()
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    query = f'''
    SELECT * FROM stock_data 
    WHERE Symbol = "{symbol}"
    AND Date >= "{cutoff_date}"
    ORDER BY Date
    '''
    df = pd.read_sql_query(query, conn)
    df['Date'] = pd.to_datetime(df['Date'])
    return df

# Sidebar for controls
st.sidebar.header("Dashboard Controls")

# Stock selector
selected_symbol = st.sidebar.selectbox("Select Stock", SYMBOLS)

# Time range selector
time_range = st.sidebar.slider(
    "Select Time Range (days)",
    min_value=7,
    max_value=365,
    value=30,
    step=1
)

# Load the selected data
try:
    df = load_data(selected_symbol, time_range)
    
    if df.empty:
        st.warning("No data available for the selected stock and time range. Please run the pipeline first.")
        st.stop()
    
    # Display basic stock info
    if not df.empty:
        latest = df.iloc[-1]
        
        # Create metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            delta = latest['Close'] - latest['Open']
            delta_color = "normal" if delta >= 0 else "inverse"
            st.metric(
                label="Current Price",
                value=f"${latest['Close']:.2f}",
                delta=f"{delta:.2f} Today",
                delta_color=delta_color
            )
        
        with col2:
            daily_return = latest['Daily_Return'] * 100 if 'Daily_Return' in latest else 0
            st.metric(
                label="Daily Return",
                value=f"{daily_return:.2f}%",
                delta=None
            )
        
        with col3:
            st.metric(
                label="Volume",
                value=f"{latest['Volume']:,}",
                delta=None
            )
        
        with col4:
            st.metric(
                label="RSI (14)",
                value=f"{latest['RSI_14']:.2f}",
                delta=None
            )
        
        # Create tabs for different visualizations
        tab1, tab2 = st.tabs(["Price Charts", "Technical Indicators"])
        
        with tab1:
            # Create candlestick chart
            fig = make_subplots(
                rows=2, 
                cols=1, 
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=("Price", "Volume"),
                row_heights=[0.7, 0.3]
            )
            
            # Add candlestick chart
            fig.add_trace(
                go.Candlestick(
                    x=df['Date'],
                    open=df['Open'],
                    high=df['High'],
                    low=df['Low'],
                    close=df['Close'],
                    name="Price"
                ),
                row=1, col=1
            )
            
            # Add Moving Average
            fig.add_trace(
                go.Scatter(
                    x=df['Date'],
                    y=df['MA5'],
                    name="5-Day MA",
                    line=dict(color='orange', width=1)
                ),
                row=1, col=1
            )
            
            # Add volume
            fig.add_trace(
                go.Bar(
                    x=df['Date'],
                    y=df['Volume'],
                    name="Volume",
                    marker_color='rgba(0, 128, 255, 0.5)'
                ),
                row=2, col=1
            )
            
            # Update layout
            fig.update_layout(
                height=600,
                xaxis_rangeslider_visible=False,
                title=f"{selected_symbol} Stock Price and Volume",
                yaxis_title="Price (USD)",
                yaxis2_title="Volume"
            )
            
            # Display the chart
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Create technical indicators chart
            fig = make_subplots(
                rows=2, 
                cols=1, 
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=("Price Range Percentage", "RSI (14)"),
                row_heights=[0.5, 0.5]
            )
            
            # Add Price Range Percentage
            fig.add_trace(
                go.Scatter(
                    x=df['Date'],
                    y=df['Price_Range_Pct'],
                    name="Price Range %",
                    line=dict(color='purple', width=1)
                ),
                row=1, col=1
            )
            
            # Add RSI
            fig.add_trace(
                go.Scatter(
                    x=df['Date'],
                    y=df['RSI_14'],
                    name="RSI (14)",
                    line=dict(color='blue', width=1)
                ),
                row=2, col=1
            )
            
            # Add RSI reference lines
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
            
            # Update layout
            fig.update_layout(
                height=500,
                title=f"{selected_symbol} Technical Indicators",
                yaxis_title="Price Range %",
                yaxis2_title="RSI"
            )
            
            # Display the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Add description
            st.markdown(""
                Indicators Explanation:
                Price Range Percentage: Shows daily price volatility (High-Low)/Open × 100
                **RSI (Relative Strength Index)**: Momentum oscillator that measures the speed and change of price movements
                ** RSI > 70: Potentially overbought conditions
                ** RSI < 30: Potentially oversold conditions
                "")
            
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Please make sure you have run the pipeline and data is available in the database.")
"""
        
        # Write the streamlit app file
        with open("streamlit_app.py", "w") as f:
            f.write(streamlit_code)