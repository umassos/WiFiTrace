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
# Objective : This file is the main file that has all functions/imports for preprocessing raw syslog files and
#  extract sessions of each device with the APs. The session file created is further used for contact tracing.

# Imports
import os
import presence_msg as pmsg
import sys
import getopt
import datetime as dt
import user_device_role as unamedevmap
import session

# Directories, prefix and postfix strings in fname

idir=""

# This function calls appropriate functions for presence message extraction from the unzipped file
def presence_msg_extract(ifile):
    # Extract presence messages from the syslog file
    ofile = pmsg.presence(ifile)

    # Now, extract authentication messages : event <522008> from the ofile returned above
    authfile = pmsg.auth_msg(ofile)
    return(ofile,authfile)

def main():
    input_dir = ""
    output_dir = ""
    year = dt.datetime.now().year

    # Read command line parameters : i and o for input and output dirs
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hy:i:o:", ["help", "year=","input=", "output="])
    except getopt.GetoptError:
        print("main.py -y <YYYY> -i <input> -o <output>")
        sys.exit(2)

    #print(opts)
    for opt, arg in opts:
        if opt == '-h':
            print("main.py -y <YYYY> -i <input> -o <output>")
            sys.exit()
        else:
            if opt in ("-y", "--year"):
                year = arg
            if opt in ("-i", "--input"):
                input_dir = arg
            if opt in ("-o", "--output"):
                output_dir = arg

    #print(year, input_dir, output_dir, output_dir.rsplit("/",1)[0])

    #print("-------------------------Hello World--------------------------")
    print("**** Trajectory Extraction of new file starts here *****")

    indir=""
    ifile_regex = ""

    if os.path.isdir(input_dir):
        print(">>> Input is a directory, processing all files in the input dir")
        indir = input_dir
        ifile_regex = ""
        print(indir, ifile_regex)
    elif os.path.isfile(input_dir):
        print(">>> Input file exists, processing the file")
        indir = input_dir.rsplit("/", 1)[0] + "/"
        ifile_regex = input_dir.rsplit("/", 1)[1]
        print(indir, ifile_regex)
    else:
        print("Enter Valid Input File name")
        return(0)

    ofile_flag = False
    if os.path.isdir(output_dir):
        print(">>> Output file name is a directory")
    elif os.path.isfile(output_dir):
        ofile_flag = True
        print(">>> Output file already exists, Overwriting it")
    else:
        print("Enter Valid Output File name")
        return(0)

    # go through all files in the raw_data directory and process them
    for file in os.listdir(indir):
        ifile = indir + file
        if (os.path.isfile(ifile) and (file.startswith(ifile_regex))):
            print("\n----- Processing File : %s ------\n" %ifile)
            presence_msg_file , auth_file = presence_msg_extract(ifile)
            print("Presence Message, Auth init File Names are: %s %s\n" %(presence_msg_file, auth_file))

            # Extract usernames from the auth_file above
            # auth_file name looks as above : /<complete path>/<fname>_auth_event.txt

            ##########################################################
            # Create the device username role mapping
            ##########################################################
            try:
                unamedevmap.extract_authfields(auth_file)
            except:
                pass

            ########################################################
            # Now, compute sessions from the presence messages
            ########################################################
            # Extract fields needed for computing sessions.
            session_init_fname = session.extract_session_items(presence_msg_file)
            print(session_init_fname)

            ################################################################
            # Final step: Create sessions
            # The output is a csv file with 7 fields :
            # MAC ID, Date, Month, Year, AP_Name, Start Time, End Time
            ################################################################
            ofile = session.get_sessions(session_init_fname, year, output_dir, ofile_flag)

    print("Session File created")

    print("\n Now, Cleaning up :")

    tmp_dir = "/tmp"
    dir = ifile.rsplit("/", 2)[0] + tmp_dir

    try:
        # Delete the tmp dir if it exists
        if os.path.isdir(dir):
            os.removedirs(dir)
            print("\nTmp Files Deleted\n")

    except:
        print("Some temp files don't exist, deleted earlier")

if __name__ == "__main__":
    main()
