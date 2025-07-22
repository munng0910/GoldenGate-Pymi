#!/bin/sh

# ==============================
# Oracle eDelivery Secure Download Script
# ==============================

# Set language
LANG=C
export LANG

# Trap to cleanup cookie file in case of unexpected exits.
trap 'cleanup_and_exit' 1 2 3 6 

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
WGET="/usr/bin/wget"

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

LOGFILE="$LOGDIR/wgetlog-$(date +%Y-%m-%d-%H:%M:%S).log"

# Create temporary cookie file
COOKIE_FILE=$(mktemp -t wget_sh_XXXXXX) 
if [ $? -ne 0 ] || [ -z "$COOKIE_FILE" ]; then 
    echo "Error: Failed to create temporary cookie file. See $LOGFILE for details." | tee -a "$LOGFILE" 
    exit 1 
fi 

# Output directory
OUTPUT_DIR="."

# Extract filename from URL
SOURCEURL=$(echo "$URL" | awk -F'/' '{print $3}')
if [[ "$SOURCEURL" == "download.oracle.com"]]; then
    FILENAME=$(echo "$URL" | awk -F'/' '{print $NF}')
else
    FILENAME=$(echo "$URL" | sed -n 's/.*fileName=\([^&]*\).*/\1/p')
fi

# ==============================
# Function Definitions
# ==============================

cleanup_and_exit() {
    [ -f "$COOKIE_FILE" ] && rm -f "$COOKIE_FILE"
    echo "Cleaned up temporary cookie file." >> "$LOGFILE"
    exit 1
}

# Authenticate user
authenticate() {
    echo "Authenticating with Oracle eDelivery..." | tee -a "$LOGFILE"
    $WGET --secure-protocol=auto --save-cookies="$COOKIE_FILE" --keep-session-cookies \
        --http-user "$SSO_USERNAME" --password "$SSO_PASSWORD" "https://edelivery.oracle.com/osdc/cliauth" -a "$LOGFILE"
    
    if [ $? -ne 0 ]; then 
        echo "Error: Authentication failed. Check $LOGFILE for details." | tee -a "$LOGFILE"
        cleanup_and_exit
    fi
    echo "Authentication successful." | tee -a "$LOGFILE"
}

# Download file
download_file() {
    echo "Downloading file from: $URL" | tee -a "$LOGFILE"
    $WGET --load-cookies="$COOKIE_FILE" "$URL" -O "$OUTPUT_DIR/$FILENAME" >> "$LOGFILE" 2>&1

    if [ $? -ne 0 ]; then 
        echo "Error: File download failed. Check $LOGFILE for details." | tee -a "$LOGFILE"
        cleanup_and_exit
    fi

    echo "Download complete: $OUTPUT_DIR/$FILENAME" | tee -a "$LOGFILE"
}

# ==============================
# Main Execution
# ==============================

authenticate
download_file
cleanup_and_exit

