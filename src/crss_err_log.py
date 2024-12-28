import datetime
import traceback

def error_log(message, error):
    """
    For logging errors in the ETL process.\n
    message-(str) indicates what part of process failed.\n
    error-(str) indicates the error that occured.\n
    example:\n
    try:
        ......
    except Exception as e:
        error_log("step that failed", e)
    """
    # for error logging
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H:%M:%S")
    error_log = "C:/Users/cojac/Projects/crss/src/ERROR_LOG.txt"
    with open(error_log, "a") as f:
        f.write(f"----------------\n{timestamp}  --->  {message}\n{error}\n")
        traceback.print_exc(file=f) # include traceback
