import requests
import time
#  important authentication

nifi_server = "localhost"
nifi_port = 8443
username = "username"
password = "saksham84achaurasia"

# Processors id's

get_file_processor='5075c4e5-018a-1000-f43a-9e07c201d3a7'
put_hdfs_processor='5075dbd2-018a-1000-c685-d20e1b497813'

#  To solve the error of certificate error
requests.packages.urllib3.disable_warnings()



# getting access token

#  url for access token
url = f"https://{nifi_server}:{nifi_port}/nifi-api/access/token"


headers1 = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
}

# This is data payload or we can say access_payload
data = {
    "username": username,
    "password": password
}

#  Post request for access token
response = requests.post(url, headers=headers1, data=data, verify=False)
access_token = response.text


def checking_response(response):
    if response.status_code >= 200 and response.status_code <=204:
        print("successfully executed:")
    else:
        print("Failed to obtain access token.")
        print("Response status code:", response.status_code)
        print("Response content:", response.text)

# ------------------------------------------------
# For access_token
checking_response(response)

# Define the headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}
# ----------------------------------------
def processor_response(res,program,file,sec):
    if res.status_code == 200:
        processor_details = res.json()
        revision_id = processor_details["revision"]["version"]
        
    #     defining the payload to start the processor
        payload = {
            "state": program,
            "revision": {
                "version": revision_id
            }
        }
        delay_seconds = sec
        print(f"Waiting for {delay_seconds} seconds before stopping the processor...")
        time.sleep(delay_seconds)
        
    #     now put request to start it
        response = requests.put(file, headers=headers, json=payload, verify=False)
        
        checking_response(response)
    else:
        print("Failed to fetch processor details.")
        print("Response status code:", res.status_code)
        print("Response content:", res.text)
    
# -------------------------------------------------------

get_file = f"https://{nifi_server}:{nifi_port}/nifi-api/processors/{get_file_processor}"

start_get_file = f"https://{nifi_server}:{nifi_port}/nifi-api/processors/{get_file_processor}/run-status"

stop_get_file = f"https://{nifi_server}:{nifi_port}/nifi-api/processors/{get_file_processor}/run-status"

def start_get_file_processor():
    # For GetFile---start and stop by using nifi rest api --First we need to get it and then start it

    get_file_response = requests.get(get_file, headers=headers, verify=False)

    processor_response(get_file_response,"RUNNING",start_get_file,0)


def stop_get_file_processor():

    # Stopping the get file processor

    get_file_response = requests.get(get_file, headers=headers, verify=False)

    processor_response(get_file_response,"STOPPED",stop_get_file,40)

# ---------------------------------------------------------------------------------
# For GetFile---start and stop by using nifi rest api --First we need to get it and then start it
put_hdfs = f"https://{nifi_server}:{nifi_port}/nifi-api/processors/{put_hdfs_processor}"

start_hdfs = f"https://{nifi_server}:{nifi_port}/nifi-api/processors/{put_hdfs_processor}/run-status"

stop_hdfs = f"https://{nifi_server}:{nifi_port}/nifi-api/processors/{put_hdfs_processor}/run-status"

def start_put_hdfs_processor():
    # Now put hdfs processor 

    put_hdfs_response = requests.get(put_hdfs, headers=headers, verify=False)

    processor_response(put_hdfs_response,"RUNNING",start_hdfs,0)
    


def stop_put_hdfs_processor():
    # Stopping the put hdfs file processor

    put_hdfs_response = requests.get(put_hdfs, headers=headers, verify=False)

    processor_response(put_hdfs_response,"STOPPED",stop_hdfs,120)
