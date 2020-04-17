# Author : Amee Trivedi
# Objective : In this file we have functions to extract presence messages from the syslog

# Imports
import os


# Some globals
tmp_dir = "/tmp"
presence_dir = "/presence_msg/"
user_dir = "/Users/"

presence_event =  [
                   "<501044>", "<501098>", "<501080>", "<501081>", "<501106>",
                   "<501009>", "<501110>", "<501111>", "<501093>", "<501094>",
                   "<501099>", "<501109>", "<501101>", "<501102>", "<501104>",
                   "<501112>", "<501113>", "<501100>", "<501107>", "<501108>",
                   "<501114>", "<501117>", "<501126>", "<501129>", "<501130>",
                   "<501128>", "<501105>", "<501118>", "<501119>", "<501120>",
                   "<501121>", "<501122>", "<501123>", "<501124>", "<501125>",
                   "<501127>", "<501130>", "<501082>", "<501085>", "<501090>",
                   "<501095>", "<501097>", "<501092>", "<501084>", "<501087>",
                   "<501088>", "<501089>","<522008>"
                  ]
error_ = ['|AP ']

# Special Authentical messages : 133005, 132019

#======================================================================================
# Remove the errorneous messages
# Helper Function to presence events
#======================================================================================
def cleanup(infile):
    print("In Clean up function")
    outfile = infile.rsplit("_",1)[0] + ".txt"
    with open(infile) as oldfile, open(outfile, 'w') as newfile:
        for line in oldfile:
            if not any(error in line for error in error_):
                newfile.write(line)
    os.remove(infile)
    print(">>> Deleted the temp presence file", infile)
    return(outfile)

#===============================================================================
# Extract all presence events
#===============================================================================
def presence(ifile):
    #print("*** In presence ***\n")
    fname = ifile.rsplit("/",1)[1].rsplit(".", 1)[0]
    dir = ifile.rsplit("/",2)[0]

    # Now, if the presence msg folder doesn't exist create it
    pdir = dir + tmp_dir + presence_dir
    print(">>> Presence Msg Dir name is %s\n" %pdir)
    if not os.path.exists(pdir):
        # No dir, then create one
        os.makedirs(pdir)

    # Check if the final _presence.txt file exists , if it does :
    # return from here:
    if os.path.exists(pdir + fname + "_presence.txt"):
        cleanup_file = pdir + fname + "_presence.txt"
        print(">>> Presence file %s already present \n" % cleanup_file)
        return  cleanup_file

    outfile = pdir + fname + "_presence_temp"
    #print outfile

    with open(ifile, encoding="ISO-8859-1") as oldfile, open(outfile, 'w') as newfile:
        for line in oldfile:
            try:
                if any(event in line for event in presence_event):
                    newfile.write(line)
            except:
                continue

    # Remove any erroneous messages
    cleanup_file = cleanup(outfile)
    print(">>> Presence file %s created \n" % cleanup_file)
    return(cleanup_file)

#===============================================================================
# Extract all authentication events
#===============================================================================
def auth_msg(ifile):
    print("In auth_msg procedure")
    auth_event = '522008'
    # Now, if the users dir doesn't exist create it
    idir = ifile.rsplit(presence_dir,1)[0] + user_dir
    print(">>> Auth Msg Dir name is %s\n" %idir)
    if not os.path.exists(idir):
        os.makedirs(idir)

    # Get the filename
    outfile_name = ifile.rsplit(presence_dir,1)[1].split("_presence")[0]
    ofile = idir + outfile_name + "_auth_event.txt"

    # Check if the final _auth_event.txt file exists , if it does not then create else just return
    if not os.path.exists(ofile):
        # Now, that the dir exists, create the file and save it
        with open(ifile) as infile, open(ofile, 'w') as outfile:
            for line in infile:
                if auth_event in line:
                    #print("***** In auth_msg procedure *****")
                    #print(line)
                    outfile.write(line)
        print(">>> Auth file %s created \n" %ofile)
    else:
        print(">>> Auth file %s already present \n" %ofile)
    return(ofile)