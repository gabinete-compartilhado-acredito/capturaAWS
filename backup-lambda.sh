# This script is ran without any parameters.
# It downloads information from AWS Lambda functions and their codes
# (currently only the $LATEST version, I think), and save them locally
# in a pre-defined (hard-coded) folders.
#
# Written by Henrique S. Xavier, hsxavier@gmail.com, on 09/jul/2019.


# Hard-coded stuff:
lambda_dir=lambda
info_dir=$lambda_dir/INFO
function_list_file=function-list.json
aliases_dir=aliases

# Save list of functions
echo "Getting function list and storing in $function_list_file..."
aws lambda list-functions > $info_dir/$function_list_file


# Download info (and code) for each function:
echo "Loop over functions:"
function_list=`grep FunctionName $info_dir/$function_list_file | cut -d: -f2 | cut -d\" -f2`
for f in $function_list; do
    echo "  $f"
    
    # Get aliases:
    echo "    Get aliases..."
    aws lambda list-aliases --function-name $f > $info_dir/$aliases_dir/$f.json
    
    # Get the code (I think from $LATEST):
    echo "    Download code..."
    code_link=`aws lambda get-function --function-name $f | grep Location | cut -d\" -f4`
    wget $code_link -O $info_dir/temp.zip
    unzip -o $info_dir/temp.zip -d $lambda_dir/$f
    
done

# Remove temp files:
rm -f $info_dir/temp.zip

