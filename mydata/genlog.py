import random
import time

start_time = 1472967000.0

def get_trans_type():
    return "T20006%02d" % random.randint(1, 21)

def get_trans_source():
    return random.choice(("1000", "1100", "1300", "1700", "1800", "1900", "2400",\
                          "2400", "3000", "3400", "5100", "5900", "7100", "8100", "8300", "8400", "8500", "8700"))

def get_trans_duration():
    return random.randint(300, 3000)/1000.0

def get_trans_return_code():
    return random.choice(("0","0","0","0","0","0","0","0","0","0","0","0","0","0",\
             "0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","2999"))

def get_trans_start_time():
    global start_time
    d = random.uniform(0,0.5)
    start_time = start_time + d
    time_array = time.localtime(start_time)
    return time.strftime("%Y-%m-%d %H:%M:%S", time_array)

def print_sql(len):
    for i in range(0,len):
        print "UPSERT INTO TESTLOG VALUES(%d, '%s', '%s', '%s', %f, '%s');" % (i, get_trans_start_time(),
            get_trans_type(),
            get_trans_source(),
            get_trans_duration(),
            get_trans_return_code());

def print_csv(len):
    for i in range(0,len):
        print "%d,%s,%s,%s,%f,%s" % (i, get_trans_start_time(),
            get_trans_type(),
            get_trans_source(),
            get_trans_duration(),
            get_trans_return_code())

print_csv(10)
