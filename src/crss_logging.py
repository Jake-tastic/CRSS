import datetime
import traceback
import os
from sys import exit as sysex

directory = "~/Projects/crss/src/CRSS_LOG.txt"
logspot = os.path.expanduser(directory)

def logging(level, message, error):
    """
    For logging errors in the ETL process.\n
    level- 1=Message, 2=Warning, 3=ALERT!
    message-(str) indicates what part of process failed.\n
    error-(str) indicates the error that occured.\n
    example:\n
    try:
        ......
    except Exception as e:
        error_log("step that failed", e)
    """
    # for error logging
    err_level = {
        3: "***ALERT!***",
        2: "*Warning*",
        1:"--Message--"
    }.get(level, "")
    
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H:%M:%S")
    with open(logspot, "a") as erlog:
        erlog.write(f"----------------\n{timestamp}\nError level: {err_level} \nMessage: {message}\nError:\n{error}\n")
        traceback.print_exc(file=erlog) # include traceback

def row_counts(file_name, raw, clean):
    """
    Count rows before and after transformation to check for discrepancies\n
    file_name- indicate the file being processed\n
    raw- the row count before transformation\n
    clean- row count after transformation
    """

    if clean < raw:
        logging(3, file_name, 
                  f"Rows lost during transformation process\nRaw Count:{raw}\nClean Count: {clean}\n Lost Count: {raw - clean}")
        sysex(1)

    elif clean > raw:
        logging(3, file_name, 
                  f"Rows created during transformation process\nRaw Count:{raw}\nClean Count: {clean}\n Added Count: {clean - raw}")
        sysex(1)

    else:
        logging(1, file_name, f"Row counts match as expected.\nRaw Count:{raw}\nClean Count: {clean}")