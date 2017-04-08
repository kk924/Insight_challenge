import sys
import os
import collections
import re
import time
import heapq
from datetime import datetime

# input_file = "log_input/log.txt"

host = "host"
timestamp = "timestamp"
request = "request"
reply = "reply"
bytes = "bytes"

# Regular expression to group the components of each line
# line_regex = r"^([^\s]+) - - (\[[^\]]+\]) (\"[^\"]+\") ([^\s]+) ([^\s]+)$"
line_regex = r"^([^\s]+) - - \[([^\]]+)\] \"(.+)\" ([^\s]+) ([^\s]+)$"
regex = re.compile(line_regex)


counter = 0
blocked_count = 0
request_regex_start = re.compile(r"^(GET|POST|HEAD)\s+")
request_regex_tail = re.compile(r"\s+(HTTP\/1.0)$")
resource_bytes_transferred = collections.defaultdict(int)
log_list = list()
host_dict = collections.defaultdict(int)
failed_login = collections.defaultdict(list)

fdict = collections.defaultdict(list)
blocked_hosts = dict()


def break_line(line):
    _data = {}
    _data[host], _data[timestamp], _data[request], _data[reply], _data[bytes] = regex.search(line).groups()
    return _data


def time_at(index):
    return datetime.strptime(log_list[index][timestamp],"%d/%b/%Y:%H:%M:%S -0400")


def time_difference(index1, index2):
    return round(float((time_at(index1) - time_at(index2)).total_seconds()/60.0),2)


def get_input_file(input_file):
    input_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', input_file))
    with open(input_file_path, "r") as f:
        data = f.readlines()
    print len(data)
    for index, line in enumerate(data):
        log_list.append(break_line(line))


def get_top_ten_items(data_dictionary):
    inverted_value_heap_list = [(-value, key) for key, value in data_dictionary.items()]
    heap_list = heapq.nsmallest(10, inverted_value_heap_list)
    return [(key, -value) for value, key in heap_list]


def login_failed(response):
    if response == "401":
        return True
    elif response == "200":
        return False


def format_blocked_output(log_entry):
    entry = log_entry[host]
    entry = entry + " - - "
    entry = entry + "[" + log_entry[timestamp] + "] "
    entry = entry + "\"" + log_entry[request] + "\""
    entry = entry + " " + log_entry[reply] + " "
    entry = entry + log_entry[bytes]
    return entry


def time_difference_in_seconds(time1, time2):
        return (time1 - time2).total_seconds()


def feature_1(feature1_output_file):
    for index, log_entry in enumerate(log_list):
        host_dict[log_entry[host]] += 1
    with open(feature1_output_file, "w") as f:
        for key, value in get_top_ten_items(host_dict):
            f.write(str(key) + "," +str(value) +"\n")


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


def feature_3(feature3_output_file):
    max_all = collections.defaultdict(list)
    start = 0
    end = 1
    counter = 0
    if len(log_list) <= 1:
        print len(log_list)
    else:

        while end < len(log_list):
            if end % 10000 == 0:
                counter += 1
                print "...", counter

            if time_difference(end, start) <= 60.0:
                pass
            if time_difference(end, start) > 60.0 or end == len(log_list) - 1:
                if len(max_all.keys()) < 10:
                    min_threshold = 1
                else:
                    min_threshold = sorted(max_all.keys())[0]
                if time_difference(end - 1, start) <= 60.0 or start == end - 1:
                    if end == len(log_list) - 1:
                        max_all[end - start + 1].append(log_list[start][timestamp])
                    elif end - start >= min_threshold:
                        max_all[end - start].append(log_list[start][timestamp])
                    if len(max_all.keys()) > 10:
                        # max_all.pop(min_threshold, None)
                        pass
                while start <= end and time_difference(end, start) > 60.0:
                    start += 1

            end += 1
    file_write_count = 0
    with open(feature3_output_file, "w") as f:
        print max_all
        for k,v in sorted(max_all.items(), key=lambda x: x[0], reverse=True)[:10]:
            for value in v:
                print "writing this", str(value) + "," + str(k) +"\n"
                f.write(str(value) + "," + str(k) +"\n")
                file_write_count += 1
                if file_write_count == 10:
                    break
            if file_write_count == 10:
                break
        f.write("\n")


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
    # if len(argv) == 5:
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