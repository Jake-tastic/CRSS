import datetime
import traceback

def error_log(level, message, error):
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
    err_level = ()
    if level == 3:
        err_level = ("***ALERT!***")
    elif level == 2:
        err_level = ("*Warning*")
    elif level == 1:
        err_level = ("--Message--")
    else:
        err_level = ()
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H:%M:%S")
    error_log = "C:/Users/cojac/Projects/crss/src/ERROR_LOG.txt"
    with open(error_log, "a") as f:
        f.write(f"----------------\n{timestamp}  --->\nError level: {err_level} \nMessage: {message}\n{error}\n")
        traceback.print_exc(file=f) # include traceback
