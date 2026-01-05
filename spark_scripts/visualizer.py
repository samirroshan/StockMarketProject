import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import os

# REPLACE YOUR OLD FILE_PATH WITH THIS:
base_dir = os.path.dirname(os.path.abspath(__file__))
FILE_PATH = os.path.join(base_dir, "..", "data", "live_stock_averages.csv")

print(f"🔍 Visualizer looking for data at: {FILE_PATH}")

def animate(i):
    if not os.path.exists(FILE_PATH):
        print("Waiting for CSV file..t {FILE_PATH}....")
        return
    try:
        df = pd.read_csv(FILE_PATH)
        
        if df.empty:
            return
        df['end'] = pd.to_datetime(df['end']).dt.strftime('%H:%M:%S')
        df = df.sort_values('end').tail(20)
        
        plt.cla()
      
        for symbol in df['symbol'].unique():
             symbol_df = df[df['symbol'] == symbol]
             plt.plot(symbol_df['end'], symbol_df['moving_avg_price'], marker='o', label=symbol)
       
        plt.title('Live Stock Moving Averages (Spark Output)', fontsize=14)
        plt.xlabel('Time (Window End)', fontsize=10)
        plt.ylabel('Average Price ($)', fontsize=10)
        plt.xticks(rotation=45)
        plt.legend(loc='upper left')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
    except Exception as e:
        print(f"Error reading data: {e}")

# Create the animation (updates every 1000ms / 1 second)
ani = FuncAnimation(plt.gcf(), animate, interval=1000, cache_frame_data=False)

print("📈 Launching Live Visualizer...")
plt.show()
