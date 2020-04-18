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
# Objective : All functions needed for contact tracing

# Imports
import pandas as pd
import os
from datetime import datetime, timedelta
import ast
import gc

# Global Paths
idir = ""
mac_name_file = ""

patient_stat_ap_traj = []
patient_stat_ap_traj_start = []
patient_stat_ap_traj_end = []
patient_stat_ap_traj_duration = []
sliding_window = 0
flist = []

AP_traj_list = []
AP_traj_start_list = []
AP_traj_end_list = []
AP_duration = []
Bldg_traj_list = []
Bldg_traj_start_list = []
Bldg_traj_end_list = []
Bldg_duration = []

theta = 0

tmp_dir = "tmp_session/"

# Helper function to compute past n days
def get_past_n_days(numdays, date):
    past_n_day_list = []

    year = date[:4]
    month = date[4:6]
    date_val = date[6:]
    d = year + "-" + month + "-" + date_val

    cr_date = datetime.fromisoformat(d)
    print("In get_past_n_days ", cr_date)
    #cr_date = date(int(year), int(month), int(date_val))
    #print(cr_date)

    sdate = cr_date - timedelta(days=int(numdays))
    print(d)

    for i in range(int(numdays) + 1):
        day = sdate + timedelta(days=i)
        past_n_day_list.append(day.strftime("%Y%m%d"))

    print(past_n_day_list)
    return(past_n_day_list)

# This function checks for validity of the arguments
def valid_args(patient_mac, numdays, date):
    # Check if the args are valid

    # Flags
    patient_mac_flag = False
    numdays_flag = False
    date_flag = False
    return_flag = False

    """
    # Commenting out for the time till I add the entire trajectory dataset with mac_id information file
    mac_ifile = idir + mac_name_file
    df = pd.read_csv(mac_ifile)
    if patient_mac in df["MAC"].tolist()
        patient_mac_flag = True
    else:
        print(">> Entered MAC ID ***NOT*** present in the database \n")
    """
    patient_mac_flag = True

    if int(numdays) < 1 :
        print(">> Enter number of days for backtrace greater than 0 \n")
    else:
        numdays_flag = True

    """
    year = int(date[:4])
    month = int(date[4:6])
    date_val = int(date[6:])

    cr_date = datetime(year, month, date_val)
    print(cr_date)
    d_iso = date.fromisoformat(datetime(year, month, date_val))
    print(d_iso)
    #except:
    #    print(">> Invalid date entered \n")

    date_flag = True
    """

    year = int(date[:4])
    month = int(date[4:6])
    date_val = int(date[6:])

    if (year > 2015) and (year < 2020):
        if (month > 0) and (month < 13):
            if (date_val > 0) and (date_val < 31):
                date_flag = True
            else:
                print(">> Invalid date of the month \n")
        else:
            print(">> Invalid month of the year \n")
    else:
        print(">> Year not present in the WiFi log database \n")

    if (patient_mac_flag and numdays_flag and date_flag):
        return_flag = True

    return(return_flag)

def get_colocation(row):
    overlap_ap = []
    overlap_start = []
    overlap_end = []
    overlap_duration = []
    coloc_flag = 0

    global patient_stat_ap_traj
    global patient_stat_ap_traj_start
    global patient_stat_ap_traj_end

    # Current AP Traj details
    # 'AP_Trajectory', 'AP_Traj_Start', 'AP_Traj_End', 'AP_Duration',
    ap_traj = ast.literal_eval(row["AP_Trajectory"])
    ap_traj_start = ast.literal_eval(row["AP_Traj_Start"])
    ap_traj_end = ast.literal_eval(row["AP_Traj_End"])
    ap_traj_duration = ast.literal_eval(row["AP_Duration"])

    #print(len(ap_traj), len(patient_stat_ap_traj))

    if len(patient_stat_ap_traj) > 0 and len(ap_traj) > 0:
        for i in range(0, len(ap_traj)):
            item = ap_traj[i]
            #print(item, len(ap_traj), patient_stat_ap_traj)
            if item == "UNKN":
                continue
            #print(row)
            for j in range(0, len(patient_stat_ap_traj)):
                overlap_endtime = -1
                overlap_starttime = -1
                #print(item, patient_stat_ap_traj[j])
                if item == patient_stat_ap_traj[j]:
                    # Check time of co-location
                    item_start_time = int(ap_traj_start[i])
                    item_end_time = int(ap_traj_end[i])
                    item_time = []
                    patient_time = []

                    for k in range(int(item_start_time), int(item_end_time)+1):
                        item_time.append(k)
                    #print(int(patient_stat_ap_traj_start[i]), int(patient_stat_ap_traj_end[i])+1)
                    for k in range(int(patient_stat_ap_traj_start[j]), (int(patient_stat_ap_traj_end[j]) + (int(sliding_window)*60))+1):
                        patient_time.append(k)
                    if(not set(item_time).isdisjoint(patient_time)):
                        #print(row)
                        if (patient_time[0] < item_time[0]):
                            overlap_starttime = item_time[0]
                        else:
                            overlap_starttime = patient_time[0]
                        if (patient_time[-1] < item_time[-1]):
                            overlap_endtime = patient_time[-1]
                        else:
                            overlap_endtime = item_time[-1]

                        coloc_flag = 1
                        #  Now, append the overlaps
                        overlap_ap.append(item)
                        overlap_start.append(overlap_starttime)
                        overlap_end.append(overlap_endtime)
                        #print("reached till duration")
                        overlap_duration.append(int(overlap_endtime) - int(overlap_starttime))

    total_overlap = 0
    if len(overlap_duration) > 0 :
        for w in range(0, len(overlap_duration)):
            total_overlap += overlap_duration[w]

    #if coloc_flag > 0:
        #print(row["UserName"])
        #print(coloc_flag, overlap_ap, overlap_start, overlap_end, overlap_duration, total_overlap)
        #print("Completed")
    return(coloc_flag, overlap_ap, overlap_start, overlap_end, overlap_duration, total_overlap)


# This function is used for printing the co-located user report
def get_user_report(patient_mac, odate, numdays, start_date, end_date):
    print(flist)
    df_list = []
    result = pd.DataFrame()
    result_with_patient = pd.DataFrame()

    for file_ in flist:
        df = pd.read_csv(file_, index_col=False)
        df_list.append(df)

    result = pd.concat([item for item in df_list], ignore_index=True)
    print(list(result))
    print(len(result.index))
    print(result["Date"].unique())

    # Now, print the user report by the user : most co-located to least co-located
    # Group by mac , compute total duration of stay and get the mac ids in descending order by co-location duration

    result_with_patient = result
    result = result[result["MAC"]!= patient_mac]
    df_coloc = result.groupby(['MAC'])['Total_Coloc_duration'].sum().reset_index()

    #print(df_coloc)

    # Now, ranking by co-location duration
    df_sorted = df_coloc.sort_values(by=['Total_Coloc_duration'], ascending=False)
    #print(df_sorted)

    # Now, print the summary to the file
    report_dir = idir + "reports/"

    if not os.path.exists(report_dir):
        os.mkdir(report_dir)

    user_report_fname = report_dir + "User_Report_" + patient_mac + "_" + odate + "_" + numdays + ".txt"
    f = open(user_report_fname, "a+")
    f.write("\n This is a User Report with details about all users co-located with : \n ")
    f.write("\t\t\t Patient MAC %s \n" % patient_mac)
    f.write("\t\t\t From %s till %s\n \n" %(start_date, end_date))
    #f.write("User Report related to patient mac %s \n" % patient_mac)
    f.write("*********************************************************************************************\n")
    f.write("            Summary of all co-locators (Mac ID and total co-location duration)\n")
    f.write("*********************************************************************************************\n\n")
    f.write(" Total number of campus users co-located with the infected patient %d\n" %df_sorted["MAC"].nunique())
    f.write(" Duration of co-location is %s till %s\n\n" %(start_date, end_date))
    f.write("\t\t\t MAC \t\t\t Co-location Duration(mins)\n")
    f.write("-----------------------------------------------------------\n")

    for index, row in df_sorted.iterrows():
        f.write("%27s \t %5d \n" %(row['MAC'], row['Total_Coloc_duration']))

    f.write("\n")

    # Now, for each mac print the total co-location duration followed by the date-wise trajectory details
    # Date : total Colocation duration in mins
    # Start Time, End Time, Building, AP_Name, Duration in mins

    mac_list = df_sorted["MAC"].to_list()
    f.write("Below is a list of all the : \n"
            "\t\t\t(i) locations, \n"
            "\t\t\t(ii) start times, and \n"
            "\t\t\t(iii) duration of co-location of users on campus with the infected patient \n\n")
    f.write("*********************************************************************************************\n")
    f.write("     (Detail Report) Individual Co-location and Duration List of all co-locators \n")
    f.write("*********************************************************************************************\n\n")

    for item in mac_list:
        df_item = result[result["MAC"] == item]

        f.write("Co-location Details of User with MAC ID : %s \n" % item)
        f.write("----------------------------------------------------------------------------------------------\n")
        f.write("Date \t Start Time \t End Time \t Building \t Room No. \t\t AP_Name \t Duration(mins) \n")
        f.write("----------------------------------------------------------------------------------------------\n")

        for index, row in df_item.iterrows():
            # Date
            df_date = str(row["Year"]) + str(row["Month"]) + str(row["Date"])
            #print(df_date)
            # Coloc Traj
            co_traj = ast.literal_eval(row["Coloc_traj"])
            #print(co_traj[0])
            # Start Time
            co_start = ast.literal_eval(row["Coloc_start"])
            # End Time
            co_end = ast.literal_eval(row["Coloc_end"])
            # Duration
            co_duration = ast.literal_eval(row["Coloc_duration"])

            for k in range(0, len(co_traj)):
                if co_traj[k] != "UNKN":
                    bldg = co_traj[k].split("-")[0]
                    room = co_traj[k].split("-")[1].split("-")[0]
                    start_hr = int(co_start[k] / 60)
                    start_min = int(co_start[k] % 60)
                    end_hr = int(co_end[k] / 60)
                    end_min = int(co_end[k] % 60)
                    print(co_start, start_hr, start_min)
                    print(co_end, end_hr, end_min)
                    #f.write("%9s \t %4d \t %4d \t %6s \t %9s \t %15s \t %9s \n" % (df_date, co_start[k], co_end[k],
                    #                                                                       bldg, room, co_traj[k], co_duration[k]))
                    f.write("%9s \t %4d:%2d \t %4d:%2d \t %6s \t %9s \t %15s \t %9s \n" % (df_date, start_hr, start_min, end_hr, end_min,
                                                                                   bldg, room, co_traj[k], co_duration[k]))

        f.write("\n")

    f.close()


# ["Start", "End","Session_AP_Name", "Next_Time_Diff", "MAC", "Year", "Month", "Date"]
def get_traj(row):
    global theta
    global AP_traj_list
    global AP_traj_start_list
    global AP_traj_end_list
    global AP_duration
    global Bldg_traj_list
    global Bldg_traj_start_list
    global Bldg_traj_end_list
    global Bldg_duration

    AP_traj_list1 = []
    AP_traj_start_list1 = []
    AP_traj_end_list1 = []
    AP_duration1 = []
    Bldg_traj_list1 = []
    Bldg_traj_start_list1 = []
    Bldg_traj_end_list1 = []
    Bldg_duration1 = []

    EOD = 1439 # Number of mins in a day, 0 indexed
    bldg_list = []

    ap_list = row["Session_AP_Name"]
    start_list = row["Start"]
    end_list = row["End"]

    # Now, insert UNKN's wherever needed and merge AP list where needed
    prev_end = 0
    i = 0

    # Insert UNKN where-ever needed
    while prev_end != 1439:
        if i == 0:
            prev_start = start_list[i]
            prev_end = end_list[i]
            prev_ap = ap_list[i]
            i += 1
        else:
            if start_list[i] - prev_end > 10: # Kept it as 10 to have
                #print i
                current_start = start_list[i]
                #Insert at ith position
                ap_list.insert(i, "UNKN")
                start_list.insert(i, prev_end)
                end_list.insert(i, current_start)

            prev_start = start_list[i]
            prev_end = end_list[i]
            prev_ap = ap_list[i]
            i += 1

        # Check if you are at end of list and end time is not at 1439
        if prev_end == end_list[-1]:
            # End of list and no other value beyond now
            # Insert at last position
            if EOD-prev_end < theta:
                end_list[-1] = EOD
            else:
                ap_list.insert(i, "UNKN")
                start_list.insert(i, prev_end)
                end_list.insert(i, EOD)

            prev_start = start_list[-1]
            prev_end = end_list[-1]
            prev_ap = ap_list[-1]

    #print "Added UNKN in AP List"

    # Compress contiguous AP's to a single one
    for i, n in enumerate(ap_list):
        if i == 0 or n != ap_list[i - 1]:
            AP_traj_list1.append(n)
            AP_traj_start_list1.append(start_list[i])
            AP_traj_end_list1.append(end_list[i])
        else:
            AP_traj_end_list1[-1] = end_list[i]

    #print "Compressed AP list"

    # Compute the durations at each AP
    AP_duration1 = [x-y for x,y in zip(AP_traj_end_list1, AP_traj_start_list1)]

    AP_traj_list.append(AP_traj_list1)
    AP_traj_start_list.append(AP_traj_start_list1)
    AP_traj_end_list.append(AP_traj_end_list1)
    AP_duration.append(AP_duration1)

    print(len(AP_traj_list), len(AP_traj_start_list), len(AP_traj_end_list), len(AP_duration))

def min_of_day(row):
    st = row["Start_Time"]
    et = row["End_Time"]

    st_hr, st_min = st.split(":")
    start = int(st_hr)*60 + int(st_min)

    et_hr, et_min = et.split(":")
    end = int(et_hr)*60 + int(et_min)

    return(start, end)

# This function returns the entire contact trace of patient_mac mac id
def get_trace(patient_mac, numdays, odate, w, sess_length, oformat):
    global idir
    global patient_stat_ap_traj
    global patient_stat_ap_traj_start
    global patient_stat_ap_traj_end
    global patient_stat_ap_traj_duration
    global sliding_window
    global flist
    global tmp_dir

    cols = ["MAC", "Session_AP_Name", "Year", "Month", "Date", "Start_Time", "End_Time", "Unix_Start_Time",
            "Unix_End_Time"]

    # Initiate idir value from config.txt
    # Initiate idir value from config.txt
    f = open("./config.txt")
    f.readline()
    f.readline()
    line = f.readline()
    print(line)
    idir=line.split("=")[1]

    if(len(idir)==0):
        print("Enter Valid idir name in config file")

    print(idir)

    sliding_window = w
    print(patient_mac, numdays, odate)

    # Now, that we have the 3 args we need to :
    # (i) Check if the patient_mac is a valid mac id
    # (ii) Date is a valid date and present in our database
    # (iii) numdays is a valid number
    valid_arg_ret_val = valid_args(patient_mac, numdays, odate)
    #print(valid_arg_ret_val)

    if not(valid_arg_ret_val):
        print("***** Invalid command line arguments, get_trace call ended ***** \n")
        return(False)

    # Now that we know all the input args are valid, we need to
    # Compute the past n dates from the date entered, account for change of month, year, etc
    past_n_days = get_past_n_days(numdays, odate)

    # For each day in the list of past_n_days,
    # (i) Search if the trajectory file exists
    # (ii) Check if the mac_id exists
    # (iii) Extract a list of all stationary AP level locations, start, and end time of each stationary session
    # (iv) Now, for those times extract all other mac_ids that were co-located at same time and location
    # (v) Compute overlap time
    # (vi) Per mac_id that overlapped give a sum of total overlap time across all days, per day total overlap time
    # (vii) Return it as a list


    start_date = past_n_days[0]
    end_date = past_n_days[-1]

    report_dir = idir + "reports/"

    if not os.path.exists(report_dir):
        os.mkdir(report_dir)

    patient_report_fname = report_dir + "Patient_Report_" + patient_mac + "_" + odate + "_" + numdays + ".txt"
    f = open(patient_report_fname, "a+")

    f.write("\n\nThis is the PATIENT REPORT for mac id : %s \n" % patient_mac)
    f.write("Below are the list of locations visited and time of visit by the patient from %s till %s\n \n" % (start_date, end_date))
    # f.write("MAC ID : %s \t" % patient_mac)
    # f.write("Date : %s \n" % date)
    f.write("----------------------------------------------------------------------------------------------\n")
    f.write(" Date \t  Start Time \t End Time \t Building \t Room No. \t AP_Name \t Duration(mins) \n")
    f.write("----------------------------------------------------------------------------------------------\n")

    for date in past_n_days:
        for file_ in os.listdir(idir):
            fname = idir + file_
            print(fname)
            if(fname.endswith(".csv")):
                df1 = pd.read_csv(fname, index_col=False)
            else:
                continue

            if not (df1.columns.isin(cols).all()):
                print("%s File has incorrect Format, Searching next file" %fname)
                continue
            else:
                # All cols are present, now check the date
                year = df1["Year"].unique()[0]
                mon = df1["Month"].unique()[0]
                dd = df1["Date"].unique()[0]

                if int(mon) < 10:
                    m = "0" + str(mon)
                else:
                    m = str(mon)
                if int(dd) < 10:
                    d = "0" + str(dd)
                else:
                    d = str(dd)

                date_str = str(year)+m+d
                if date_str != date:
                    print("Date %s Match NOT found in %s(session dates for %s)" %(date,fname, date_str))
                    continue
                else:
                    # Now, dates matched, so compress the sessions
                    ofile = idir + tmp_dir + file_.split(".csv")[0] + "_temp_traj.csv"
                    # Check if file already processed earlier and saved
                    if not os.path.exists(ofile) :
                        # Add Start and End cols for min of the day
                        df1["Start"], df1["End"] = zip(*df1.apply(min_of_day, axis=1))

                        # Compress the session file and create longer sessions

                        # Now, create list of the entries per MAC
                        df = df1.groupby("MAC").agg(lambda x: list(x)).reset_index()

                        del [[df1]]
                        gc.collect()
                        df1 = pd.DataFrame()

                        df["Year"] = year
                        df["Month"] = mon
                        df["Date"] = dd

                        print(df)

                        # Now, save it
                        if not os.path.exists(idir+tmp_dir):
                            os.makedirs(idir+tmp_dir)

                        temp_file = idir + tmp_dir + file_.split(".csv")[0] + "_temp_list.csv"
                        print(temp_file)
                        df.to_csv(temp_file, index=False)

                        print("Temp List File Saved\n")

                        # Now, remove the consequetive repeating elements in AP_list and compress it
                        df.apply(get_traj, axis=1)

                        print(len(AP_traj_list), len(AP_traj_start_list), len(AP_traj_end_list))
                        print(len(df.index))

                        df["AP_Trajectory"], df["AP_Traj_Start"], df["AP_Traj_End"], df[
                            "AP_Duration"] = AP_traj_list, AP_traj_start_list, AP_traj_end_list, AP_duration

                        print(">>> Session Compression Completed")

                        # Now, save it
                        df.to_csv(ofile, index=False)
                        print(df.head(5))
                    else:
                        print(ofile)
                        df = pd.read_csv(ofile, index_col=False)

                    # Check (ii)
                    mac_list = df["MAC"].tolist()
                    #print(mac_list)
                    if patient_mac in mac_list:
                        print("Patient_MAC trajectory found in file :", fname)
                        patient_stat_ap_traj = []
                        patient_stat_ap_traj_start = []
                        patient_stat_ap_traj_end = []
                        patient_stat_ap_traj_duration = []

                        # Task (iii)
                        patient_ap_traj = ast.literal_eval((df[df["MAC"] == patient_mac]["AP_Trajectory"].values.tolist())[0])
                        patient_ap_traj_start = ast.literal_eval((df[df["MAC"] == patient_mac]["AP_Traj_Start"].values.tolist())[0])
                        patient_ap_traj_end = ast.literal_eval((df[df["MAC"] == patient_mac]["AP_Traj_End"].values.tolist())[0])
                        patient_ap_traj_duration = ast.literal_eval((df[df["MAC"] == patient_mac]["AP_Duration"].values.tolist())[0])

                        #d = ast.literal_eval(patient_ap_traj_duration)
                        #print(d[0], d[1])

                        #print(patient_ap_traj, patient_ap_traj[0])
                        #print(patient_ap_traj_start)
                        #print(patient_ap_traj_end)
                        #print(patient_ap_traj_duration)

                        # Now, extract only stationary periods
                        print(len(patient_ap_traj),len(patient_ap_traj_start), len(patient_ap_traj_end), len(patient_ap_traj_duration))
                        for i in range(0, len(patient_ap_traj_duration)):
                            #print(i)
                            if int(patient_ap_traj_duration[i]) > sess_length:
                                # This is a stationary period
                                patient_stat_ap_traj.append(patient_ap_traj[i])
                                patient_stat_ap_traj_start.append(patient_ap_traj_start[i])
                                patient_stat_ap_traj_end.append(patient_ap_traj_end[i])
                                patient_stat_ap_traj_duration.append(patient_ap_traj_duration[i])
                        #print(patient_stat_ap_traj)
                        #print(patient_stat_ap_traj_start)
                        #print(patient_stat_ap_traj_end)
                        #print(patient_stat_ap_traj_duration)

                        # Now, create a patient report
                        # Print Mac ID , Date, Start Time, End Time, Building Name, Floor Number, Room Number,
                        # AP Name, Duration in mins

                        for x in range(0, len(patient_stat_ap_traj)):
                            if patient_stat_ap_traj[x] != "UNKN":
                                bldg = patient_stat_ap_traj[x].split("-")[0]
                                room = patient_stat_ap_traj[x].split("-")[1].split("-")[0]
                                start_hr = int(patient_stat_ap_traj_start[x]/60)
                                start_min = int(patient_stat_ap_traj_start[x]%60)
                                end_hr = int(patient_stat_ap_traj_end[x]/60)
                                end_min = int(patient_stat_ap_traj_end[x]%60)
                                #f.write(("{0,11} \t {2,11} \t {3,9} \t {4,9} \t {5,6} \t {6} \n".format(%(patient_stat_ap_traj_start[x],
                                #        patient_stat_ap_traj_end[x], bldg, room, patient_stat_ap_traj[x],
                                #        patient_stat_ap_traj_duration[x]))))
                                #f.write("%7s \t %8s \t %6s \t %6s \t %6s \t %s \n" %(patient_stat_ap_traj_start[x],
                                #        int(patient_stat_ap_traj_end[x]), bldg, room, patient_stat_ap_traj[x],
                                #        patient_stat_ap_traj_duration[x]))
                                f.write("%9s \t %4d:%2d \t %4d:%2d \t %6s \t %6s \t %15s \t %-6s \n" %(date, start_hr,start_min, end_hr, end_min,
                                                                                           bldg, room, patient_stat_ap_traj[x],
                                                                                            patient_stat_ap_traj_duration[x]))

                        f.write("\n")
                        print(date)

                        #print(patient_stat_ap_traj)
                        #print(len(patient_stat_ap_traj))
                        #print(patient_stat_ap_traj_start)
                        #print(patient_stat_ap_traj_end)
                        #print(patient_stat_ap_traj_duration)

                        # Task (iv)
                        # Now, extract other mac_id's co-located at the same time same place and
                        # compute co-location duration

                        df["Coloc_flag"], df["Coloc_traj"], df["Coloc_start"],df["Coloc_end"], df["Coloc_duration"], df["Total_Coloc_duration"] = zip(*df.apply(get_colocation,axis=1))

                        print("Complete")
                        df_new = df[df["Coloc_flag"]==1]

                        temp_dir = idir + "temp_df/"

                        if not os.path.exists(temp_dir):
                            os.mkdir(temp_dir)

                        if (oformat == "csv" or oformat == "CSV"):
                            ofname = temp_dir + date + "_" + patient_mac + ".csv"
                            df_new.to_csv(ofname , index=False)
                            flist.append(ofname)
                        else:
                            ofname = temp_dir + date + "_" + patient_mac + ".json"
                            df_new.to_json(ofname, index=False)
                            flist.append(ofname)

                        print(list(df_new))
                        print(df_new.head(5))

                    else:
                        f.write("%9s \t \t \t User Not on Campus \n\n" % date)
                        print("Patient_MAC trajectory not found in file \n")

    f.write("---------------------------------------------------------------------------------------------------\n")
    f.close()
    # Now, we have saved the files and file list of the co-locators
    # Print reports from these co-locator file lists
    print(flist)
    get_user_report(patient_mac, odate, numdays, start_date, end_date)

    return(0)
