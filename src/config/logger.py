import logging
import os
import time

def get_logger(name):
    
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
    
        #console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        #file
        file_handler = logging.FileHandler("logs/pipeline.log")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


def log_layer_start(logger, layer_name):
    logger.info(f"{layer_name} | ******************")
    logger.info(f"{layer_name} | Starting Layer: {layer_name}")
    logger.info(f"{layer_name} | ------------------")

def log_file_started(logger, file_name, flag, files_for_gold = None):
    if flag == "B":
        logger.info(f"BRONZE | Ingesting raw source file: {file_name}")
    elif flag == "S":
        logger.info(f"SILVER | Transforming from Bronze parquet file: {file_name}")
    elif flag == "G":
        logger.info(f"GOLD | Transforming from Silver parquet files: {files_for_gold}")
    return time.time()

def log_file_finished(logger, df, name, time, flag):
    if flag == "B":
        log_file_finished_util(logger, df, name, time, "Bronze")
    elif flag == "S":
        log_file_finished_util(logger, df, name, time, "Silver")
    elif flag == "G":
        log_file_finished_util(logger, df, name, time, "Gold")

def log_file_finished_util(logger, df, name, time, layer):
    rows = df.count()
    logger.info(f"{layer.upper()} | Finished creating {layer} parquet file: {name}")
    logger.info(f"{layer.upper()} | Total Rows: {rows}")
    logger.info(f"{layer.upper()} | Total Load Time: {time:.2f} seconds")
    if layer != "Gold":
        logger.info(f"{layer.upper()} | ------------------")

def log_layer_end(logger, layer_name):
    logger.info(f"{layer_name} | Finished layer: {layer_name}")

def get_blank_line(logger):
    for handler in logger.handlers:
        handler.stream.write("\n")
        handler.flush()