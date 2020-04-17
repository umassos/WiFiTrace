#############################################################################################
# Author : Amee Trivedi
#
# Objective : This file has functions that are used to create a user, device and role map
#
# Input file is from the /data/Users/<date>_auth_event.txt
#
# Output file is a csv file with 8 main columns, which are:
# username, MAC, IP, role, timestamp, Month, Date and AP
#############################################################################################


# Imports
import pandas as pd
import os

# Directories and prefix : Some globals
presence_dir = "/presence_msg/"
user_dir = "Users/"
notification_regex = "<NOTI>"

month = []
date = []
timestamp = []
uname = []
mac = []
ip = []
role = []
ap =[]

# This function extracts the username, MAC, IP, role, timestamp, Month, Date and AP
def extract_authfields(ifile):
    global notification_regex
    global month
    global date
    global timestamp
    global uname
    global mac
    global ip
    global role
    global ap

    month = []
    date = []
    timestamp = []
    uname = []
    mac = []
    ip = []
    role = []
    ap = []

    print(ifile)

    # Check if the final _presence.txt file exists , if it does :
    # return from here:
    ufilename = ifile.split("_auth_event")[0] + "_umap.csv"
    if os.path.exists(ufilename):
        print("UMAP file exists , returning from here \n")
        return(ufilename)

    with open(ifile) as infile:
        flag = True

        for line in infile:
            items = line.split(" ")
            #print(line)
            #print(items[6], items[0], items[1], items[2], items[13], items[14], items[15], items[16], items[17], items[18], items[19])
            #print(items[0], items[1], items[2])

            if (items[1] != "" and items[6] == notification_regex):
                if flag == True:
                    cur_date = items[1]
                    flag = False
                if items[1] == cur_date:
                    month.append(items[0])
                    date.append(items[1])
                    timestamp.append(items[2])

                    # get username
                    try :
                        uname.append(items[13].split("=")[1])
                    except:
                        uname.append("UNK")
                    try:
                        # get mac
                        mac.append(items[14].split("=")[1])
                    except:
                        mac.append("UNK")

                    try:
                        # get ip
                        ip.append(items[15].split("=")[1])
                    except:
                        ip.append("UNK")

                    try:
                        # get role
                        role.append(items[16].split("-")[-1])
                    except:
                        role.append("UNK")

                    try:
                        # get AP
                        ap.append(items[18].split("=")[-1])
                    except:
                        ap.append("UNK")
            else:
                #print(">>> items[1] == "" because %s", items[1])
                flag_process = False

                if(items[1] == ""):
                    if flag == True:
                        cur_date = items[2]
                        flag = False
                    if items[1] == cur_date:
                        month.append(items[0])
                        date.append(items[2])
                        timestamp.append(items[3])
                        flag_process = True

                elif(items[1] != "" and items[7] == notification_regex):
                    if flag == True:
                        cur_date = items[1]
                        flag = False
                    if items[1] == cur_date:
                        month.append(items[0])
                        date.append(items[1])
                        timestamp.append(items[2])
                        flag_process = True
                else:
                    print("\n *** Change in HP Aruba Event MSG log please check the format *** \n")

                if (flag_process):
                    try:
                        uname.append(items[14].split("=")[1])
                    except:
                        uname.append("UNK")
                    try:
                        # get mac
                        mac.append(items[15].split("=")[1])
                    except:
                        mac.append("UNK")

                    try:
                        # get ip
                        ip.append(items[16].split("=")[1])
                    except:
                        ip.append("UNK")

                    try:
                        # get role
                        role.append(items[17].split("-")[-1])
                    except:
                        role.append("UNK")

                    try:
                        # get AP
                        ap.append(items[19].split("=")[-1])
                    except:
                        ap.append("UNK")
            #print("****",uname[-1],mac[-1],ip[-1],role[-1],ap[-1])

    # Now, we have extracted everything we need from the authentication messages
    # Let us create a dataframe and save it to a csv file

    print(len(ap), len(role), len(ip), len(mac), len(uname), len(month), len(date), len(timestamp))

    df= pd.DataFrame(
        {'Month': month,
        'Date': date,
        'Timestamp': timestamp,
        'UserName' : uname,
        'MAC' : mac,
        'IP' : ip,
        'Role' : role,
        'AP' :ap
        })
    print(df)
    ufilename = ifile.split("_auth_event")[0] + "_umap.csv"
    df.to_csv(ufilename, index=False)
    print("Saved the UMAP file")