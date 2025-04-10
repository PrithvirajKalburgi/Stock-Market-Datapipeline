import luigi
import logging
from datetime import datetime
import os
from luigi_pipeline.config import SYMBOLS
from luigi_pipeline.visualise import VisualizeData, StreamlitVisualization

# Configure logging
logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class StockPipeline(luigi.WrapperTask):
    """
    Wrapper task to run the entire pipeline for all symbols
    """
    date = luigi.DateParameter(default=datetime.now().date())
    include_streamlit = luigi.BoolParameter(default=True)
    
    def requires(self):
        # First, ensure all stocks get visualized
        visualizations = [VisualizeData(symbol=symbol, date=self.date) for symbol in SYMBOLS]
        
        # If streamlit is enabled, add it as a dependency
        if self.include_streamlit:
            return visualizations + [StreamlitVisualization()]
        else:
            return visualizations

if __name__ == '__main__':
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # Run the pipeline
    luigi.build([StockPipeline()], local_scheduler=True)
    
    # Print instructions for starting the Streamlit app
    print("\n" + "="*80)
    print("Pipeline complete! To launch the Streamlit dashboard, run:")
    print("streamlit run streamlit_app.py")
    print("="*80 + "\n")