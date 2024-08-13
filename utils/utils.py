import time

# Utility functions
def convert_to_datetime(t):
    s = str(t)
    s = s.split('.')[0]
    if len(s) > 10:
        t = t / 1000.0    
    return time.strftime("%d-%m-%Y %H:%M:%S %p", time.localtime(t))

def convert_to_date(t):
    s = str(t)
    s = s.split('.')[0]
    if len(s) > 10:
        t = t / 1000.0
    return time.strftime("%Y-%m-%d", time.localtime(t))

def convert_to_hour(t):
    s = str(t)
    s = s.split('.')[0]
    if len(s) > 10:
        t = t / 1000.0

    start = time.localtime(t)
    end = time.localtime(t + 60*60)
    return "{} - {}".format(
        time.strftime("%I:00%p", start),
        time.strftime("%I:00%p", end)
    )

def convert_to_minute(t):    
    s = str(t)
    s = s.split('.')[0]
    if len(s) > 10:
        t = t / 1000.0
    return time.strftime("%M", time.localtime(t))    

def convert_to_day(t):
    s = str(t)
    s = s.split('.')[0]
    if len(s) > 10:
        t = t / 1000.0     
    return time.strftime("%a", time.localtime(t))
