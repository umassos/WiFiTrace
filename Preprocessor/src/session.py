#############################################################################################
# Author : Amee Trivedi
#
#  Objective : This file has functions needed to extract session information from
# the presence events.
#
# Input file is from the /data/presence_msg/<date>_presence.txt
#
# Output file is a csv file with  main columns named "<date>_session_event_ap.csv"
#############################################################################################

# Imports
import pandas as pd
import gc
import os
import datetime
import calendar
from time import mktime

# Directories and prefix : Some globals
presence_dir = "/presence_msg/"
fname_postfix = "_presence.txt"

# Some globals to be defined
month = []
date = []
timestamp = []
event_list = []
macid = []
ap_ip = []
ap_name = []
ap_bssid = []

# Empty df
df_interpolate = pd.DataFrame(columns=['AP_BSSI', 'AP_IP', 'AP_Name', 'Date',
                                       'Event', 'MAC', 'Month', 'Timestamp',
                                       'Event_Code', 'Hour', 'Min', 'Sec',
                                       'Time_in_Minutes', 'Prev_Event_Code',
                                       'Prev_Time_in_Minutes', 'Time_Diff',
                                       'Prev_AP_Name'])

# Association Events
assoc_events = [501009, 501110, 501093, 501094, 501109, 501101, 501112, 501100,
                501095, 501097, 501092, 522008]

# Dis-association events
dis_events = [501099, 501102, 501104, 501113, 501107, 501108, 501114, 501105,
              501044, 501098, 501080, 501081, 501106, 501111]

# Empty List
cols =['AP_BSSI', 'AP_IP', 'AP_Name', 'Date',
       'Event', 'MAC', 'Month', 'Timestamp',
       'Event_Code', 'Hour', 'Min', 'Sec',
       'Time_in_Minutes', 'Prev_Event_Code',
       'Prev_Time_in_Minutes', 'Time_Diff',
       'Prev_AP_Name']
lst=[]

AP_traj_list = []
AP_traj_start_list = []
AP_traj_end_list = []
AP_duration = []
Bldg_traj_list = []
Bldg_traj_start_list = []
Bldg_traj_end_list = []
Bldg_duration = []

###############################################################################
# This function extracts the needed fields from the presence event message
# and appends it to the corresponding list to create a temp dataframe
# which is saved as a csv in the end.
###############################################################################
def extract_session_items(infile):
    print(">>> Extracting session details from syslog events")
    #print(infile)

    # Check if final file exisits, if yes, return here itself:
    name_split = infile.rsplit("presence_msg/", 1)
    odir = name_split[0] + "session/"
    filename = name_split[1].split("_presence.txt")[0] + "_session_event_ap.csv"
    fpath = odir + filename

    if os.path.exists(fpath):
       print("\n%s File Exists" %fpath)
       return(fpath)

    with open(infile) as inputfile:
        flag = True
        for line in inputfile:
            items = line.split(" ")
            event = line.split("]: <")[1].split(">")[0]
            if items[1] == "":
                del items[1]
            if flag == True:
                # This is the first line so save the date
                cur_date = items[1]
                flag = False

            if items[1] == cur_date:
                # List of possible events and the cols list to choose from

                if event == "501044":
                    # Event has substring:
                    # sta:%m]: No authentication found trying to de-authenticate to BSSID [bss:%m] on AP [name:%s]
                    # For eg:
                    # Station [sta:%m]: No authentication found trying to de-authenticate to BSSID [bss:%m] on AP [name:%s]
                    event_list.append(event)
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])
                    keyword_split = line.split("Station ",1)
                    macid.append(keyword_split[1].split(" ")[0])

                    ap_item = keyword_split[1].split("BSSID ")[1].split(" ")
                    ap_bssid.append(ap_item[0])
                    ap_name.append(ap_item[-1])
                    ap_ip.append("-1")

                elif event == "501098":
                    # Events have common substring "[mac:%m]: Moved out from AP [ip:%P]-[bssid:%m]-[name:%s]"
                    # Eg.
                    # Deauth to sta: [mac:%m]: Moved out from AP [ip:%P]-[bssid:%m]-[name:%s] to new AP
                    event_list.append(event)
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])
                    keyword_split = line.split(": Moved out from AP ")
                    macid.append(keyword_split[0].split(" ")[-1])

                    try :
                        ap_item = keyword_split[1].split(" ")[0].split("-",2)
                    except :
                        ap_item = []
                        #print(line)
                    try:
                        ap_bssid.append(ap_item[1])
                    except:
                        ap_bssid.append("-1")
                    try:
                        ap_name.append(ap_item[-1])
                    except:
                        ap_name.append("-1")
                    try:
                        ap_ip.append(ap_item[0])
                    except:
                        ap_ip.append("-1")

                elif event in ["501080", "501081", "501106"]:

                    # All events have common substring "[mac:%m]: Ageout AP [ip:%P]-[bssid:%m]-[name:%s]"
                    # For eg:
                    # Auth request: [mac:%m]: AP [ip:%P]-[bssid:%m]-[name:%s] auth_alg [auth_alg:%d]
                    event_list.append(event)
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])
                    keyword_split = line.split(": Ageout AP ")
                    macid.append(keyword_split[0].split(" ")[-1])

                    try :
                        ap_item = keyword_split[1].split(" ")[0].split("-",2)
                    except :
                        ap_item = []
                        #print(line)
                    try:
                        ap_bssid.append(ap_item[1])
                    except:
                        ap_bssid.append("-1")
                    try:
                        ap_name.append(ap_item[-1])
                    except:
                        ap_name.append("-1")
                    try:
                        ap_ip.append(ap_item[0])
                    except:
                        ap_ip.append("-1")

                elif event in ["501009", "501110", "501111","501093","501094", "501099", "501109"
                            ,"501101" ,"501102","501104" ,"501112", "501113", "501100" ,"501107" , "501108"
                            ,"501114", "501117" ,"501126","501129","501130", "501128" ,"501105" ,"501118"
                            ,"501119","501120" ,"501121" ,"501122" ,"501123","501124" ,"501125"
                            ,"501127" ,"501130", "501082" ,"501085", "501090"]:

                    # All events have common substring "[mac:%m]: AP [ip:%P]-[bssid:%m]-[name:%s]"
                    # For eg:
                    # Auth request: [mac:%m]: AP [ip:%P]-[bssid:%m]-[name:%s] auth_alg [auth_alg:%d]
                    event_list.append(event)
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])
                    keyword_split = line.rsplit(": AP ",1)
                    macid.append(keyword_split[0].split(" ")[-1])

                    try :
                        ap_item = keyword_split[1].split(" ")[0].split("-",2)
                    except :
                        ap_item = []
                        #print(line)
                    try:
                        ap_bssid.append(ap_item[1])
                    except:
                        ap_bssid.append("-1")
                    try:
                        ap_name.append(ap_item[-1])
                    except:
                        ap_name.append("-1")
                    try:
                        ap_ip.append(ap_item[0])
                    except:
                        ap_ip.append("-1")

                elif event == "501095":

                    # All events have common substring "[mac:%m] (SN [sn:%d]): AP [ip:%P]-[bssid:%m]-[name:%s]"
                    # For eg:
                    #Assoc request @ [tstr:%s]: [mac:%m] (SN [sn:%d]): AP [ip:%P]-[bssid:%m]-[name:%s]
                    event_list.append(event)
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])
                    keyword_split = line.rsplit(" AP ",1)
                    macid.append(keyword_split[0].split(" (SN")[0].split(" ")[-1])

                    try :
                        ap_item = keyword_split[1].split(" ")[0].split("-",2)
                    except :
                        ap_item = []
                        #print(line)
                    try:
                        ap_bssid.append(ap_item[1])
                    except:
                        ap_bssid.append("-1")
                    try:
                        ap_name.append(ap_item[-1])
                    except:
                        ap_name.append("-1")
                    try:
                        ap_ip.append(ap_item[0])
                    except:
                        ap_ip.append("-1")

                elif event in ["501097", "501092","501084","501087", "501088", "501089"]:

                    # All events have common substring "[mac:%m]: Dropped AP [ip:%P]-[bssid:%m]-[name:%s]"
                    # For eg:
                    # Assoc request: [mac:%m]: Dropped AP [ip:%P]-[bssid:%m]-[name:%s] for STA DoS protection
                    event_list.append(event)
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])
                    keyword_split = line.rsplit(": Dropped AP ",1)
                    macid.append(keyword_split[0].split(" ")[-1])

                    try :
                        ap_item = keyword_split[1].split(" ")[0].split("-",2)
                    except :
                        ap_item = []
                        #print(line)
                    try:
                        ap_bssid.append(ap_item[1])
                    except:
                        ap_bssid.append("-1")
                    try:
                        ap_name.append(ap_item[-1])
                    except:
                        ap_name.append("-1")
                    try:
                        ap_ip.append(ap_item[0])
                    except:
                        ap_ip.append("-1")

                elif event == "522008":
                    # This is the authentication messages, which is very different
                    event_list.append(event)
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])

                    # get mac
                    try:
                        macid.append(items[14].split("=")[1])
                    except:
                        macid.append("-1")

                    # get ip
                    try:
                        ap_ip.append(items[15].split("=")[1])
                    except:
                        ap_ip.append("-1")

                    # get AP
                    try:
                        ap_name.append(items[18].split("=")[-1])
                    except:
                        ap_name.append("-1")

                    #get SSID
                    try:
                        ap_bssid.append(items[19].split("=")[-1])
                    except:
                        ap_bssid.append("-1")

                else:
                    print("*** WARNING: Event not considered: Add in user_trajectory.py : extract_traj_items function, ",event)

        # Now, we have extracted everything we need from the authentication messages
        # Let us create a dataframe and save it to a csv file

        df= pd.DataFrame(
            {'Month': month,
            'Date': date,
            'Timestamp': timestamp,
            'MAC' : macid,
            'AP_IP' : ap_ip,
            'AP_Name': ap_name,
            'AP_BSSI': ap_bssid,
            'Event' : event_list
            })

        #print(list(df))

        # Check if session directory exists
        if not os.path.exists(odir):
            os.makedirs(odir)

        df.to_csv(fpath, index=False)
        print(">>> Session event AP file saved ")
        del [[df]]
        gc.collect()
        return (fpath)

def ap_name_clean(row):
    #print(row["AP_Name"])
    name = row["AP_Name"].rstrip("\r").rstrip("\n").rstrip("\"").lstrip("\"").rstrip("\r")

    #print name, row["AP_Name"]
    return name

def get_time(row):
    date = str(row["Year"]) + str(row["Month"]) + str(row["Date"])

    #print(row["Start"])
    stime = int(row["Start"])
    stime_24hr = int(stime / 60)
    stime_min = stime % 60
    stime_format = str(stime_24hr) + str(stime_min) + str("00")
    st = datetime.datetime.strptime(date + stime_format, '%Y%m%d%H%M%S')
    stime_unix = mktime(st.timetuple())
    stime_hhmm = str(stime_24hr) + ":" + str(stime_min)

    etime = int(row["End"])
    etime_24hr = int(etime / 60)
    etime_min = etime % 60
    etime_format = str(etime_24hr) + str(etime_min) + str("00")
    et = datetime.datetime.strptime(date + etime_format, '%Y%m%d%H%M%S')
    etime_unix = mktime(et.timetuple())
    etime_hhmm = str(etime_24hr) + ":" + str(etime_min)

    #print(stime, stime_hhmm, stime_unix)
    #print(etime, etime_hhmm, etime_unix)

    return (stime_hhmm, etime_hhmm, stime_unix, etime_unix)

def add_event_code(row):
    # Authentication or Association Events
    if row["Event"] in assoc_events:
        return 1
    # De-authentication or Disassociation events
    elif row["Event"] in dis_events:
        return 2
    else:
        return -1

def min_of_day(row):
    timestamp = row["Timestamp"].split(":")
    time_min = int(timestamp[0])*60 + int(timestamp[1])
    return(timestamp[0], timestamp[1], timestamp[2], time_min)

def time_diff(row):
    if pd.isnull(row["Prev_Time_in_Minutes"]):
        return -1
    return int(row["Time_in_Minutes"])-int(row["Prev_Time_in_Minutes"])

def keep_code(row):
    if not (pd.isnull(row["Event_Code"]) or pd.isnull(row["Prev_Event_Code"])):
        if (int(row["Event_Code"]) == 2 and int(row["Prev_Event_Code"]) == 1):
            return 1
    return 0

def next_time_diff(row):
    if pd.isnull(row["Next_Time_in_Minutes"]):
        return 1440 - int(row["Time_in_Minutes"])
    return int(row["Next_Time_in_Minutes"])-int(row["Time_in_Minutes"])

def add_start_end_time(row):
    theta = 0

    if int(row["Next_Event_Code"]) == 2 and int(row["Event_Code"]) == 1 and (row["AP_Name"] == row["Next_AP_Name"]):
        return (row["AP_Name"], row["Time_in_Minutes"], row["Next_Time_in_Minutes"])
    elif int(row["Next_Time_Diff"] < theta):
        print(">>> Check This row values : %s" %row)
        return (row["AP_Name"], row["Time_in_Minutes"], row["Next_Time_in_Minutes"])
    elif row["AP_Name"] == "UNKN":
        return ("UNKN", row["Time_in_Minutes"], row["Next_Time_in_Minutes"])
    else:
        return (row["AP_Name"], row["Time_in_Minutes"], row["Time_in_Minutes"]+1)

def get_sessions(infile, year, output_dir, ofile_flag):
    global df_interpolate
    gc.enable()

    ofile = infile.split(".csv")[0] + "_code_fields.csv"
    print(ofile)
    if not os.path.isfile(ofile):
        df = pd.read_csv(infile)
        print(list(df))
        #['AP_BSSI', 'AP_IP', 'AP_Name', 'Date', 'Event', 'MAC', 'Month', 'Timestamp']

        # Clean AP_Name
        df.dropna(subset=['AP_Name'], inplace=True)
        df["AP_Name"] = df.apply(ap_name_clean, axis = 1)

        # Drop all thoses rows where MAC == -1 or AP_Name == -1
        df.drop(df.loc[df['MAC'] == "-1"].index, inplace=True)

        df.drop(df.loc[df['AP_Name'] == "-1"].index, inplace=True)

        # Now, add event code
        print(df["Event"].unique())
        df["Event_Code"] = df.apply(add_event_code, axis = 1)

        # The event_code should be either a 1 or a 2 for association or dis-association,
        # if -1 we need to get rid of it.
        df_no_event = df[df["Event_Code"] == -1]
        print("Next Line should be empty list")
        print(df_no_event["Event"].unique())

        # Now, add a time in minutes column
        df["Hour"], df["Min"], df["Sec"], df["Time_in_Minutes"] = zip(*df.apply(min_of_day, axis = 1))

        # Clean the AP name columns if it has some hash value
        # Some AP names have a #string# format instead of building name, we will get rid of those rows
        df = df[~df['AP_Name'].str.match("#")]
        print(len(df.index), df["MAC"].nunique())
        print(df["AP_Name"].nunique())

        # Now, add a column that gives the earlier time_in_mins
        # and then find the time difference in mins between 2 consecutive timestamps
        df['Prev_Time_in_Minutes'] = df.groupby(['MAC'])['Time_in_Minutes'].shift(1)
        df['Prev_AP_Name'] = df.groupby(['MAC'])['AP_Name'].shift(1)

        df['Time_Diff'] = df.apply(time_diff, axis = 1)

        # Now, group the rows by MAC and add a lag column that gives the earlier event_code
        df['Prev_Event_Code'] = df.groupby(['MAC'])['Event_Code'].shift(1)
        df["Keep_Event_Code"] = df.apply(keep_code, axis = 1) # 1 is keep , 0 is UNK

        # Save the df
        ofile = infile.split(".csv")[0] + "_code_fields.csv"

        df.to_csv(ofile, index=False)
        print("Created the _code_fields.csv file")
    else:
        # File already exists read it
        df = pd.read_csv(ofile, index_col=False)

    gfile = infile.split(".csv")[0] + "_group_list_temp.csv"
    if not os.path.isfile(gfile):
        # Now, drop the columns not needed for computing sessions
        df.drop(["Hour","Min", "Sec", "Event", "AP_BSSI", "AP_IP", "Prev_AP_Name"], axis=1, inplace=True)
        gc.collect()

        # Multiple row manipulation
        # Get Next event Code from subsequent row and next row Time in Mins

        df['Next_Event_Code'] = df.groupby(['MAC'])['Event_Code'].shift(-1)
        df['Next_Time_in_Minutes'] = df.groupby(['MAC'])['Time_in_Minutes'].shift(-1)
        df['Next_AP_Name'] = df.groupby(['MAC'])['AP_Name'].shift(-1)
        df['Next_Time_Diff'] = df.apply(next_time_diff, axis = 1)

        # Fill Nan by -1
        df["AP_Name"].fillna("UNKN", inplace=True)
        df["Event_Code"].fillna(-1,inplace=True)
        df["Next_Event_Code"].fillna(-1,inplace=True)
        df['Next_Time_in_Minutes'].fillna(-1, inplace=True) # For last rows per MAC
        df["Next_AP_Name"].fillna(-1, inplace=True) # For last rows of each MAC

        # Now, add start and end columns
        df["Session_AP_Name"], df["Start"], df["End"] = zip(*df.apply(add_start_end_time, axis = 1))

        df.drop(['Time_in_Minutes', 'AP_Name', 'Event_Code', 'Prev_Time_in_Minutes', 'Time_Diff', 'Prev_Event_Code', 'Keep_Event_Code', 'Next_Event_Code', 'Next_Time_in_Minutes', 'Next_AP_Name', 'Next_Time_Diff'], axis=1, inplace=True)

        # Now, group the rows by MAC, Start Time
        df_g = df.sort_values("End", ascending=False).groupby(["MAC", "Start"]).first().reset_index()

        gfile = infile.split(".csv")[0] + "_group_list_temp.csv"
        df_g.to_csv(gfile, index=False)

    else:
        # File exists read it
        df_g = pd.read_csv(gfile, index_col=False)

    mon = df["Month"].unique()[0]
    dd = df["Date"].unique()[0]

    mmmtomm = {name: num for num, name in enumerate(calendar.month_abbr) if num}

    mm = mmmtomm[mon]

    del [[df]]
    gc.collect()
    df = pd.DataFrame()

    # Columns needed are MAC, Date, Month, Year, AP_Name, Start, End
    df_new = df_g.loc[:, df_g.columns.isin(["Start", "End","Session_AP_Name", "MAC"])]

    del [[df_g]]
    gc.collect()
    df_g = pd.DataFrame()

    df_new["Year"] = year
    df_new["Month"] = mm
    df_new["Date"] = dd

    # Convert Start Time and End time from mins of day to HH:MM format 0-24 Hr clock
    # and unix time as well

    df_new["Start_Time"], df_new["End_Time"], df_new["Unix_Start_Time"], df_new["Unix_End_Time"] = zip(*df_new.apply(get_time,axis=1))
    print(df_new)

    df_new.drop(["Start", "End"], axis=1, inplace=True)

    # Now, save it
    if ofile_flag:
        df_new.to_csv(output_dir, index=False)
    else:
        listfile = output_dir + infile.rsplit("/",1)[1].rsplit(".csv",1)[0] + "_sessions.csv"
        df_new.to_csv(listfile, index=False)

    print("\n>>>> Session File Saved\n")