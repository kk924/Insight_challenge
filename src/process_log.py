import sys
import os
import collections
import re
import time
import heapq
from datetime import datetime
import datetime as dt

# input_file = "log_input/log.txt"

host = "host"
timestamp = "timestamp"
request = "request"
reply = "reply"
bytes = "bytes"

# Regular expression to group the components of each line
# Regular expression to distinguish and parse the components of each line
# host, timestamp, request, response, and bytes
# line_regex = r"^([^\s]+) - - (\[[^\]]+\]) (\"[^\"]+\") ([^\s]+) ([^\s]+)$"
line_regex = r"^([^\s]+) - - \[([^\]]+)\] \"(.+)\" ([^\s]+) ([^\s]+)$"
regex = re.compile(line_regex)


counter = 0
blocked_count = 0

# Regular Expression to identify the type of message, and the parse the request
# component of the log entry accordingly
request_regex_start = re.compile(r"^(GET|POST|HEAD)\s+")
request_regex_tail = re.compile(r"\s+(HTTP\/1.0)$")
# Dictionary to store the sum of all bytes transferred for each resource
resource_bytes_transferred = collections.defaultdict(int)

# List to store each line in the input file as a log entry broken down into a dictionary
# The dictionary is defined using 5 keys:
# host, timestamp, request, response, and bytes
log_list = list()

# Dictionary to store the number of times a user/IP request hits the server
host_dict = collections.defaultdict(int)

# Dictionary to store the counter which monitors the number of failed login attempts by each IP address
failed_login = collections.defaultdict(list)

# Dictionary to store the blocked the IP addresses which are added here after 3 consecutive failed login attempts
# It stores the IP with the start time of the block period
blocked_hosts = dict()

# Dictionary to store the maximum number of requests over a 60 minute period.
# The dictionary stores the number of requests received and the start time.
max_all = collections.defaultdict(list)


# Breaks a line parsed from the input file into the various fields:
# host, timestamp, request, response, and bytes
# Input: A line from log.txt input file
# Output: A dictionary with the 5 keys mentioned along with their values
def break_line(line):
    _data = {}
    _data[host], _data[timestamp], _data[request], _data[reply], _data[bytes] = regex.search(line).groups()
    return _data


# Returns the time at the given input timestamp provided in the log_entry
# Converts the text timestamp into a datetime structure
def time_at(index):
    return datetime.strptime(log_list[index][timestamp],"%d/%b/%Y:%H:%M:%S -0400")


# Returns the time difference between 2 log entries based on their timestamps.
# The log entries are identified by their respective indexes/order in the input file
def time_difference(index1, index2):
    return round(float((time_at(index1) - time_at(index2)).total_seconds()/60.0),2)


# Retrieves the data in the input file as per argv[1]
def get_input_file(input_file):
    input_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', input_file))
    with open(input_file_path, "r") as f:
        data = f.readlines()
    print len(data)
    for index, line in enumerate(data):
        log_list.append(break_line(line))


# Retrieves the top 10 items from a dictionary based on the value parameter.
# Input: Any dictionary with a numerical value
def get_top_ten_items(data_dictionary):
    inverted_value_heap_list = [(-value, key) for key, value in data_dictionary.items()]
    heap_list = heapq.nsmallest(10, inverted_value_heap_list)
    return [(key, -value) for value, key in heap_list]



# Returns True/False if a login attempt was successful or failed according to the log entry
# 401 -> True(Failed)
# 200 -> False(Successful)
def login_failed(response):
    if response == "401":
        return True
    elif response == "200":
        return False


# Formats the output text to write blocked log entries to file for feature 4
def format_blocked_output(log_entry):
    entry = log_entry[host]
    entry = entry + " - - "
    entry = entry + "[" + log_entry[timestamp] + "] "
    entry = entry + "\"" + log_entry[request] + "\""
    entry = entry + " " + log_entry[reply] + " "
    entry = entry + log_entry[bytes]
    return entry


# Returns the time difference between two datetime structures in seconds
def time_difference_in_seconds(time1, time2):
        return (time1 - time2).total_seconds()


# Implements Feature 1
def feature_1(feature1_output_file):
    for index, log_entry in enumerate(log_list):
        host_dict[log_entry[host]] += 1
    with open(feature1_output_file, "w") as f:
        for key, value in get_top_ten_items(host_dict):
            f.write(str(key) + "," +str(value) +"\n")


# Implements Feature 2
def feature_2(feature2_output_file):
    for index, log_entry in enumerate(log_list):
        if log_entry[bytes] == '-':
            log_bytes = 0
        else:
            log_bytes = int(log_entry[bytes])
        if request_regex_start.search(log_entry[request]) is not None:
            if request_regex_tail.search(log_entry[request]) is not None:
                resource_key = "".join(log_entry[request].split()[1:len(log_entry[request].split())-1])
            else:
                resource_key = "".join(log_entry[request].split()[1:len(log_entry[request].split())])
        else:
            if request_regex_tail.search(log_entry[request]) is not None:
                resource_key = "".join(log_entry[request].split()[0:len(log_entry[request].split())-1])
            else:
                resource_key = "".join(log_entry[request].split()[0:len(log_entry[request].split())])
        resource_bytes_transferred[resource_key] += log_bytes

    with open(feature2_output_file, "w") as f:
        for key, value in get_top_ten_items(resource_bytes_transferred):
            f.write(key +"\n")


# Implements Feature 3
def feature_3(feature3_output_file):

    file_write_count = 0

    max_all = collections.defaultdict(list)
    hourly_request_counter = collections.defaultdict(list)
    start = 0
    start_time = time_at(start)
    end_time = time_at(len(log_list)-1)
    end = 1
    counter = 0

    while start_time < end_time:
        pointer = 0

        while time_at(pointer) < start_time and pointer < len(log_list):
            pointer += 1

        start = pointer

        while time_at(pointer) < start_time + dt.timedelta(0,3600) and pointer < len(log_list)-1:
            pointer += 1

        end = pointer

        start_time_text = start_time.strftime("%d/%b/%Y:%H:%M:%S") + " -0400"
        hourly_request_counter[end-start+1].append(start_time_text)

        if len(hourly_request_counter.keys()) > 10:
            hourly_request_counter.pop(min(sorted(hourly_request_counter.keys())), None)

        start_time = start_time + dt.timedelta(0, 1)

    with open(feature3_output_file, "w") as f:
        for k,v in sorted(hourly_request_counter.items(), key=lambda x: x[0], reverse=True)[:10]:
            for value in v:
                # print "writing this", str(value) + "," + str(k) +"\n"
                f.write(str(value) + "," + str(k) +"\n")
                file_write_count += 1
                if file_write_count == 10:
                    break
            if file_write_count == 10:
                break

    pass


# Implements Feature 4
def feature_4(feature4_output_file):
    with open(feature4_output_file, "w") as f:
        blocked_count = 0
        for index, log_entry in enumerate(log_list):
            if log_entry[host] in blocked_hosts:
                if time_difference_in_seconds(time_at(index), blocked_hosts[log_entry[host]]) <= 300:
                    # Add to blocked list
                    # print "Blocked :", log_entry
                    f.write(format_blocked_output(log_entry) +"\n")
                    blocked_count += 1
                    continue
                else:
                    blocked_hosts.pop(log_entry[host], None)

            if "POST" in log_entry[request] and "login" in log_entry[request]:
                if login_failed(log_entry[reply]):
                    if log_entry[host] not in failed_login or len(failed_login[log_entry[host]]) == 0:
                        failed_login[log_entry[host]].append(time_at(index))
                        pass
                    else:
                        if time_difference_in_seconds(time_at(index), failed_login[log_entry[host]][0]) > 20:
                            del failed_login[log_entry[host]]
                            failed_login[log_entry[host]].append(time_at(index))
                        else:
                            if len(failed_login[log_entry[host]]) == 2:
                                blocked_hosts[log_entry[host]] = time_at(index)
                                del failed_login[log_entry[host]]
                            else:
                                failed_login[log_entry[host]].append(time_at(index))
                    pass
                else:
                    # Login succeeded
                    if log_entry[host] in failed_login or len(failed_login[log_entry[host]]) > 0:
                        failed_login.pop(log_entry[host], None)
        print blocked_count


def main(argv):
    time_start = time.time()

    print  argv
    input_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', argv[1]))
    feature1_output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', argv[2]))
    feature2_output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', argv[3]))
    feature3_output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', argv[4]))
    feature4_output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', argv[5]))

    # print input_file
    get_input_file(input_file)
    print "Got data: ", time.time() - time_start

    t = time.time()
    feature_1(feature1_output_file)
    print "F1: ", time.time() - t

    t = time.time()
    feature_2(feature2_output_file)
    print "F2: ", time.time() - t

    t = time.time()
    feature_3(feature3_output_file)
    print "F3: ", time.time() - t

    t = time.time()
    feature_4(feature4_output_file)
    print "F4: ", time.time() - t

    print "Total time: ", time.time() - time_start

if __name__ == "__main__":
    main(sys.argv)