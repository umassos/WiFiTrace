"""
BSD 3-Clause License

Copyright (c) 2020, UMass Laboratory for Advanced Systems Software All rights reserved.

Redistribution and use in source and binary forms, with or without modification, 
are permitted provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following 
    disclaimer in the documentation and/or other materials provided with the distribution.

    Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products 
    derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, 
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

# Author : Amee Trivedi

# Objective : Given a mac id and number of days find the list of mac id's that were co-located at the same place
# same time with the given mac id, along with total minutes (stationary + transition) list of minutes per day for
# those n days.

# Consider only the stationary periods of the given mac id

# Imports
import pandas
import os
from datetime import datetime, timedelta
import sys
import getopt
import contact_trace as ct

def main(argv):
    #print("Hello World")

    patient_mac = ""
    end = ""
    start = ""
    w = 0
    sess_length = 0
    oformat = "csv"

    # Read command line parameters
    # mac id : mostly patient mac id
    # start date for contact tracing
    # end date for contact tracing
    # w : window size for co-location overlap : Window size of 0 means strict co-location
    # same place , same time

    # Note on changes in cmd line args and compatibility with code written for earlier args
    # Earlier arguments were mac, num_days, backtrace date and window.
    # To incorporate the new cmd args and make it compatible
    # with the code, compute numdays from start and end date and
    # pass it to get_trace function.

    # Number of days you want to back trace
    # Date from which you want to back trace, this will be the last date in the trace

    try:
        opts, args = getopt.getopt(argv, "hm:s:e:w:l:f:", ["macid=", "start=", "end=", "min_session_length=", "output_format=", "window="])
    except getopt.GetoptError:
        print('main.py -m <mac id> -s <start date> -e <end date> -l <min_session_length> -f <output_format csv/JSON> -w <window size in hrs>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('main.py -m <mac id> -s <start date> -e <end date> -l <min_session_length> -f <output_format csv/JSON> -w <window size in hrs>')
            sys.exit()
        else:
            if opt in ("-m", "--macid"):
                patient_mac = arg
            if opt in ("-s", "--start"):
                start = arg
            if opt in ("-e", "--end"):
                end = arg
            if opt in ("-w", "--window"):
                w = arg
            if opt in ("-l", "--min_session_length"):
                sess_length = arg
            if opt in ("-f", "--output_format"):
                oformat = arg

    print(patient_mac, start, end,  w)

    s_year = start[:4]
    s_month = start[4:6]
    s_date_val = start[6:]
    sd = s_year + "-" + s_month + "-" + s_date_val

    start_date = datetime.fromisoformat(sd)
    print("Start Date ", start_date)

    e_year = end[:4]
    e_month = end[4:6]
    e_date_val = end[6:]
    ed = e_year + "-" + e_month + "-" + e_date_val

    end_date = datetime.fromisoformat(ed)
    print("End Date ", end_date)
    numdays = (end_date - start_date).days

    print("Mac ID is ", patient_mac)
    print("Number of Days for Contact Tracing ", numdays)
    print("BackTrace Date is ", ed)
    print("Start Date is", sd)
    print("Sliding Window size in hrs is ", w)
    print("Minimum Session Length is", sess_length)
    print("Save output format in ", oformat)

    # Now, we have the needed parameters to trace contacts
    ret_val = ct.get_trace(patient_mac, str(numdays), end, w, int(sess_length), oformat)
    print("get_trace retval :", ret_val)

if __name__ == "__main__":
    main(sys.argv[1:])
