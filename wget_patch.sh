#!/bin/sh
 
# ==============================
# Oracle Support Secure Download Script
# ==============================

# Set language
LANG=C
export LANG
 
# Trap to cleanup cookie file in case of unexpected exits.
trap 'rm -f $COOKIE_FILE; exit 1' 1 2 3 6

# Validate that URL argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <download_url>"
    exit 1
fi

URL="$1"  # Set URL from command-line argument

# Ask for SSO Username
printf 'Enter SSO Username: '
read -r SSO_USERNAME

# Ask for SSO Password (hidden input)
printf 'Enter SSO Password: '
read -rs SSO_PASSWORD
echo ""

# Path to wget command
WGET=/usr/bin/wget

# Check if wget exists
if ! command -v "$WGET" > /dev/null 2>&1; then
    echo "Error: wget not found. Please install wget and try again."
    exit 1
fi

# Check log directory and file
LOGDIR=./log
if [ ! -d "$LOGDIR" ]; then
    echo "Log directory $LOGDIR does not exist. Creating it..."
    mkdir -p "$LOGDIR"
else
    echo "Using existing log directory $LOGDIR"
fi

if [ $? -ne 0 ]; then
    echo "Error: Failed to create log directory $LOGDIR. Please check permissions."
    exit 1
fi  

LOGFILE=$LOGDIR/wgetlog-$(date +%m-%d-%y-%H:%M).log

# Print wget version info
echo "Wget version info:
------------------------------
$($WGET -V)
------------------------------" > "$LOGFILE" 2>&1
 
# Location of cookie file
COOKIE_FILE=$(mktemp -t wget_sh_XXXXXX) >> "$LOGFILE" 2>&1
if [ $? -ne 0 ] || [ -z "$COOKIE_FILE" ]
then
echo "Temporary cookie file creation failed. See $LOGFILE for more details." |  tee -a "$LOGFILE"
exit 1
fi
echo "Created temporary cookie file $COOKIE_FILE" >> "$LOGFILE"
 
# Output directory and file
OUTPUT_DIR=.

# Extract filename from URL
FILENAME=$(echo "$URL" | awk -F'=' '{print $NF}')

#
# End of user configurable variable
#

# The following command to authenticate uses HTTPS. This will work only if the wget in the environment
# where this script will be executed was compiled with OpenSSL. 
#
$WGET  --secure-protocol=auto --save-cookies="$COOKIE_FILE" --keep-session-cookies  --http-user "$SSO_USERNAME" --password "$SSO_PASSWORD"  "https://updates.oracle.com/Orion/Services/download" -O /dev/null 2>> "$LOGFILE"
 
# Verify if authentication is successful
if [ $? -ne 0 ]
then
echo "Authentication failed with the given credentials." | tee -a "$LOGFILE"
echo "Please check logfile: $LOGFILE for more details."
else
echo "Authentication is successful. Proceeding with downloads..." >> "$LOGFILE"
 
$WGET  --load-cookies="$COOKIE_FILE" "$URL" -O "$OUTPUT_DIR/$FILENAME"  >> "$LOGFILE" 2>&1
 
fi
 
# Cleanup
rm -f "$COOKIE_FILE"
echo "Removed temporary cookie file $COOKIE_FILE" >> "$LOGFILE"