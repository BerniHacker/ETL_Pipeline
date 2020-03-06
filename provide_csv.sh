# This is a script that allows copying files from a origin directory
# into a destination directory with a configurable delay.

# Defining the variables used in provide_csv script
# Subdirectories of the directory containing this script
ORIGIN="test_orig/"
DESTINATION="test_dest/"
# Time in seconds between 2 consecutive copy commands
DELAY=10

# Getting the start time
start_time=$(date +"%T")  # HH:MM:SS format
start_time_s=$(date +%s)  # seconds format
# Printing the script's start time
echo "Script's start time: $start_time"

# Looping through all the files in the origin folder
for file in $ORIGIN*.csv
do
    # Extracting the file name with extension
    file_name=$(basename "$file")
    # Copying the file
    cp $ORIGIN$file_name $DESTINATION$file_name
    # Providing consolle feedback
    echo "The file $file_name has been copied"
    # Waiting
    sleep $DELAY
done

# Getting the end time
end_time=$(date +"%T")
end_time_s=$(date +%s)
# Calculating the duration
dur=$(($end_time_s - $start_time_s))
# Printing the script's end time
echo "Script's end time: $end_time"
# Printing the script's duration
echo "Script's duration:"
printf '%02d:%02d:%02d\n' $(($dur/3600)) $(($dur%3600/60)) $(($dur%60))
