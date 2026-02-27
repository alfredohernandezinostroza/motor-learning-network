from hamilton import driver
from constants import RAW_DATA_PATH, FIGURES_PATH
import pandas as pd
import hamilton
import logging

WOS_RAW_PATH = RAW_DATA_PATH / "wos"

hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

def list_of_wos_dataframes(wos_data_path):
    wos = []
    for i in range(45):
        try:
            wos.append(pd.read_csv(wos_data_path/f"savedrecs({i}).txt", sep="\t"))
        except:
            logger.info(f"error in line {i}!!")

if __name__ == "__main__":
    outputs = ["list_of_wos_dataframes"]
    inputs = dict(wos_data_path=WOS_RAW_PATH)
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .build()
    )
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(FIGURES_PATH/"{__file__}.png",keep_dot=True,)
    dr.visualize_execution(outputs, inputs=inputs,output_file_path=FIGURES_PATH/"{__file__}.png",keep_dot=False)
    # dr.execute(outputs, inputs=inputs)



