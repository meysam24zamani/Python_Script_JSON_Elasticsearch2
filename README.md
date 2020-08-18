# Python_Script_JSON_Elasticsearch2
 In this repository, I have created an inserting method for direct insertion of JSON format file to the Elasticsearch. 

In case you want to check it out, the final script for uploading large files (at least 1.5Gb) to ElasticSearch with python is the one you will find attached here (works with Python >= 3.3).

You can use it like this, using the JSON file also attached at this email, and it will upload the records to an index called "variants_test-meysam_47":

export BIOMED_ENV=test-meysam && python3 json_to_elk.py --project-id 47 --task-id tool_0001234 --elk-url 'https://888afce593444c7e87fc23868e40ca95.eu-west-1.aws.found.io:9243/' --elk-username 'elastic' --elk-password 'ekiQM9uillM22Y76R3YfCpco' --json-file toy.json
 
The terminal output will be very short, but if you check at your /tmp directory, you will find a /tmp/json_to_elk_task0001234.log file with extra information returned from the POST to ELK :)


( you also need to install elasticsearch Python library first, or the script will fail!

pip3 install --user elasticsearch)
