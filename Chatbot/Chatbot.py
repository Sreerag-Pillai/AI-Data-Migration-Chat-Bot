import openai
import json
#from google.colab import userdata
import os
#from google.colab import files
import uuid


openai.api_key = "API_KEY"

class DataMigrationChatbot:
    def __init__(self):
        self.conversation_history = []
        self.few_shot_examples = self.load_few_shot_examples()
        self.initial_prompt = self.get_initial_prompt()

    def load_few_shot_examples(self):
      few_shot_examples = [
        {
            "dialogue": [
                {"bot": "Hello! I'm here to assist you with transferring data. Could you please tell me the type of your data source?", "user": "Sure, it's a MySQL database."},
                {"bot": "Great! To connect to your source MySQL database, I need the username. What's your source database username?", "user": "The username is 'admin'."},
                {"bot": "Thanks. Could you provide the password for the 'admin' user? This is necessary to access the database.", "user": "The password is 'admin123'."},
                {"bot": "Understood. What's the host address of your MySQL database? This is usually an IP address or a URL.", "user": "It's hosted on 'localhost'."},
                {"bot": "And the port number? The default MySQL port is 3306.", "user": "Yes, it's 3306."},
                {"bot": "Could you tell me the name of the database you want to work with?", "user": "The database name is 'toyu'."},
                {"bot": "Great! Now, what is the name you'd like to give to this source connection?", "user": "Let's call it 'Source1'."},
                {"bot": "What is the table name in 'test_db' that you want to transfer?", "user": "The table name is 'users'."},
                {"bot": "Do you need to apply any transformations to the data? For example, converting text to uppercase.", "user": "No"},
                {"bot": "Understood. Now, let's set up the destination. What type of database is your destination?", "user": "It's also a MySQL database."},
                {"bot": "To connect to your destination MySQL database, I need the username. What's your destination database username?", "user": "The username is 'admin'."},
                {"bot": "Thanks. Could you provide the password for the 'admin' user? This is necessary to access the database.", "user": "The password is 'admin123'."},
                {"bot": "Understood. What's the host address of your MySQL database? This is usually an IP address or a URL.", "user": "It's hosted on 'localhost'."},
                {"bot": "And the port number? The default MySQL port is 3306.", "user": "Yes, it's 3306."},	
                {"bot": "Could you tell me the name of the database you want to work with?", "user": "The database name is 'test_toyu'."},				
                {"bot": "What's the name you'd like to give to this destination connection?", "user": "We'll call it 'Destination1'."},
                {"bot": "What is the table name in the destination database where you want to store the data?", "user": "The table name is 'modified_user'."},
				{"bot": "What columns from source need to be moved to the destination table?", "user": "Columns userId,fname,lname from the source need to be moved to userId,fname,lname in the destination."},
                {"bot": "Do you have any custom queries to add, or should we proceed without them?", "user": "No custom queries. We can proceed."},
                {"bot": "Would you like to schedule this transfer to happen Daily, Weekly, Monthly, or just once?", "user": "Let's schedule it to run weekly."},
                {"bot": "Great! Before I generate the JSON configuration, do you have any questions or additional details to add?", "user": "No, that covers everything."},
                {"bot": "Perfect! I'll generate the JSON configuration now."}     
            ],
            "mapping_json": {
                "pipelineId": "e7654bac-9375-4ee2-8c7c-0d02fb132c17",
                "scheduleInterval": "Weekly",
                "startDate": "08/13/2024",
                "catchup": False,
                "jobs": [
                    {
                        "id": "a89cfaff-bb18-4d10-9029-da9c6b5da08d",
                        "source": {
                            "sourceName": "Source1",
                            "sourceType": "MYSQL",
                            "sourceProperties": {
                                "taskId": "477d619d-cd4e-4882-99fb-60649748d053",
                                "connectionId": "5130100e-24e3-49f9-9855-20d7a362aebe",
                                "tableName": "users",
                                "customQuery": ""
                            }
                        },
                        "destinations": [
                            {
                                "destinationName": "Destination1",
                                "destinationType": "MYSQL",
                                "destinationProperties": {
                                    "taskId": "43ddf2e9-64e2-48b0-9653-71f1acfe932f",
                                    "connectionId": "4120100e-24e3-49f9-9855-20d7a362aecd",
                                    "tableName": "modified_user",
                                    "customQuery": ""
                                }
                            }
                        ],
                        "joinMetadata": [
                            {
                                "joinMetadataTaskId": "680f2677-67dc-4576-b3c9-5548e2da8c26",
                                "sourceMetadataId": "0b05489c-6c6e-41ad-943a-a4f81ec3012f",
                                "destinationMetadataId": "79435f35-a370-4ebf-8bc3-fa5b638cacb8",
                                "sourceColumnName": "userId,fname,lname",
                                "destinationColumnName": "userId,fname,lname",
                                "sourceGroupId": "",
                                "destinationTaskId": "43ddf2e9-64e2-48b0-9653-71f1acfe932f",
                                "sourceTaskId": "",
                                "transformations": {}
                            }
                        ]
                    }
                ],
                "sequence": {
                    "a89cfaff-bb18-4d10-9029-da9c6b5da08d": []
                },
                "taskSequence": {
                    "477d619d-cd4e-4882-99fb-60649748d053": ["43ddf2e9-64e2-48b0-9653-71f1acfe932f"]
                }
            },
            "source_connection_details": {
                "conn_id": "5130100e-24e3-49f9-9855-20d7a362aebe",
                "conn_type": "MYSQL",
                "login": "admin",
                "password": "admin123",
                "host": "localhost",
                "port": 3306,
                "database": "toyu"
            },
            "dest_connection_details": {
                "conn_id": "4120100e-24e3-49f9-9855-20d7a362aecd",
                "conn_type": "MYSQL",
                "login": "admin",
                "password": "admin123",
                "host": "localhost",
                "port": 3306,
                "database": "test_toyu"
            }
        },
        {
            "dialogue": [
                {"bot": "Hello! I'm here to assist you with transferring data. Could you please tell me the type of your data source?", "user": "Sure, it's a MySQL database."},
                {"bot": "Great! To connect to your source MySQL database, I need the username. What's your source database username?", "user": "The username is 'admin'."},
                {"bot": "Thanks. Could you provide the password for the 'admin' user? This is necessary to access the database.", "user": "The password is 'admin123'."},
                {"bot": "Understood. What's the host address of your MySQL database? This is usually an IP address or a URL.", "user": "It's hosted on 'localhost'."},
                {"bot": "And the port number? The default MySQL port is 3306.", "user": "Yes, it's 3306."},
                {"bot": "Could you tell me the name of the database you want to work with?", "user": "The database name is 'toyu'."},
                {"bot": "Great! Now, what is the name you'd like to give to this source connection?", "user": "Let's call it 'Source1'."},
                {"bot": "What is the table name in 'test_db' that you want to transfer?", "user": "The table name is 'users'."},
                {"bot": "Do you need to apply any transformations to the data? For example, converting text to uppercase.", "user": "No"},
                {"bot": "Understood. Now, let's set up the destination. What type of database is your destination?", "user": "It's also a MySQL database."},
                {"bot": "To connect to your destination MySQL database, I need the username. What's your destination database username?", "user": "The username is 'admin'."},
                {"bot": "Thanks. Could you provide the password for the 'admin' user? This is necessary to access the database.", "user": "The password is 'admin123'."},
                {"bot": "Understood. What's the host address of your MySQL database? This is usually an IP address or a URL.", "user": "It's hosted on 'localhost'."},
                {"bot": "And the port number? The default MySQL port is 3306.", "user": "Yes, it's 3306."},	
                {"bot": "Could you tell me the name of the database you want to work with?", "user": "The database name is 'test_toyu'."},				
                {"bot": "What's the name you'd like to give to this destination connection?", "user": "We'll call it 'Destination1'."},
                {"bot": "What is the table name in the destination database where you want to store the data?", "user": "The table name is 'modified_user'."},
				{"bot": "What columns from source need to be moved to the destination table?", "user": "All columns"},
                {"bot": "Do you have any custom queries to add, or should we proceed without them?", "user": "No custom queries. We can proceed."},
                {"bot": "Would you like to schedule this transfer to happen Daily, Weekly, Monthly, or just once?", "user": "Let's schedule it to run weekly."},
                {"bot": "Great! Before I generate the JSON configuration, do you have any questions or additional details to add?", "user": "No, that covers everything."},
                {"bot": "Perfect! I'll generate the JSON configuration now."}     
            ],
            "mapping_json": {
                "pipelineId": "e7654bac-9375-4ee2-8c7c-0d02fb132c17",
                "scheduleInterval": "Weekly",
                "startDate": "08/13/2024",
                "catchup": False,
                "jobs": [
                    {
                        "id": "a89cfaff-bb18-4d10-9029-da9c6b5da08d",
                        "source": {
                            "sourceName": "Source1",
                            "sourceType": "MYSQL",
                            "sourceProperties": {
                                "taskId": "477d619d-cd4e-4882-99fb-60649748d053",
                                "connectionId": "5130100e-24e3-49f9-9855-20d7a362aebe",
                                "tableName": "users",
                                "customQuery": ""
                            }
                        },
                        "destinations": [
                            {
                                "destinationName": "Destination1",
                                "destinationType": "MYSQL",
                                "destinationProperties": {
                                    "taskId": "43ddf2e9-64e2-48b0-9653-71f1acfe932f",
                                    "connectionId": "4120100e-24e3-49f9-9855-20d7a362aecd",
                                    "tableName": "modified_user",
                                    "customQuery": ""
                                }
                            }
                        ],
                        "joinMetadata": [
                            {
                                "joinMetadataTaskId": "680f2677-67dc-4576-b3c9-5548e2da8c26",
                                "sourceMetadataId": "0b05489c-6c6e-41ad-943a-a4f81ec3012f",
                                "destinationMetadataId": "79435f35-a370-4ebf-8bc3-fa5b638cacb8",
                                "sourceColumnName": "",
                                "destinationColumnName": "",
                                "sourceGroupId": "",
                                "destinationTaskId": "43ddf2e9-64e2-48b0-9653-71f1acfe932f",
                                "sourceTaskId": "",
                                "transformations": {}
                            }
                        ]
                    }
                ],
                "sequence": {
                    "a89cfaff-bb18-4d10-9029-da9c6b5da08d": []
                },
                "taskSequence": {
                    "477d619d-cd4e-4882-99fb-60649748d053": ["43ddf2e9-64e2-48b0-9653-71f1acfe932f"]
                }
            },
            "source_connection_details": {
                "conn_id": "5130100e-24e3-49f9-9855-20d7a362aebe",
                "conn_type": "MYSQL",
                "login": "admin",
                "password": "admin123",
                "host": "localhost",
                "port": 3306,
                "database": "toyu"
            },
            "dest_connection_details": {
                "conn_id": "4120100e-24e3-49f9-9855-20d7a362aecd",
                "conn_type": "MYSQL",
                "login": "admin",
                "password": "admin123",
                "host": "localhost",
                "port": 3306,
                "database": "test_toyu"
            }
        },
        {
            "dialogue": [
                {"bot": "Hello! I'm here to help you move your data. What type of data source are you using?", "user": "I'm using a MongoDB database."},
                {"bot": "Great! To connect to your MongoDB database, I need the database name. What's the name of your database?", "user": "The database name is 'customer_db'."},
                {"bot": "Thanks. Could you provide the username for accessing 'customer_db'?", "user": "The username is 'mongoUser'."},
                {"bot": "And the password for 'mongoUser'?", "user": "The password is 'mongoPass456'."},
                {"bot": "Understood. What's the host address of your MongoDB database? This is usually an IP address or a URL.", "user": "It's hosted on '192.168.2.50'."},
                {"bot": "What is the port number? The default MongoDB port is 27017.", "user": "Yes, it's 27017."},
                {"bot": "What would you like to name this source connection?", "user": "Let's name it 'MongoSource'."},
                {"bot": "Which collection in 'customer_db' do you want to transfer?", "user": "The collection name is 'clients'."},
                {"bot": "Do you need to apply any transformations to the data? For example, changing date formats.", "user": "Yes, please change the 'signup_date' to 'MM/DD/YYYY' format."},
                {"bot": "Got it. Now, let's set up where you want to send the data. What type of destination would you like?", "user": "I'd like to send it to an Excel spreadsheet."},
                {"bot": "What is the name you'd like to give to this destination file?", "user": "Let's call it 'Clients_Data.xlsx'."},
                {"bot": "Where should the Excel file be saved? Please provide the file path.", "user": "Save it to 'D:/Data/Clients_Data.xlsx'."},
                {"bot": "Would you like to schedule this transfer to happen Daily, Weekly, Monthly, or just once?", "user": "Let's schedule it to run daily."},
                {"bot": "Do you have any custom queries to add, or should we proceed without them?", "user": "No custom queries. Proceed as is."},
                {"bot": "Before I generate the JSON configuration, do you have any questions or additional details to add?", "user": "No, everything is clear."},
                {"bot": "Perfect! I'll generate the JSON configuration now."}
            ],
            "mapping_json": {
                "pipelineId": "g2345hij-6789-klmn-0123-opqrstuvwx45",
                "scheduleInterval": "Daily",
                "startDate": "10/01/2024",
                "catchup": True,
                "jobs": [
                    {
                        "id": "d78gh90i-jk12-3456-lmno-7890pqrst123",
                        "source": {
                            "sourceName": "MongoSource",
                            "sourceType": "MONGODB",
                            "sourceProperties": {
                                "taskId": "mnop4567-ef89-0123-ghij-klmnopqr4567",
                                "connectionId": "conn-mongo1-def2-ghi3-jkl4mnop5678",
                                "collectionName": "clients",
                                "customQuery": ""
                            }
                        },
                        "destinations": [
                            {
                                "destinationName": "ExcelDestination",
                                "destinationType": "EXCEL",
                                "destinationProperties": {
                                    "taskId": "qrst8901-uv23-4567-wxyz-0123abcd4567",
                                    "connectionId": "conn-mongo1-def2-ghi3-jkl4mnop5690",
                                    "filePath": "D:/Data/Clients_Data.xlsx",
                                    "customQuery": ""
                                }
                            }
                        ],
                        "joinMetadata": [
                            {
                                "joinMetadataTaskId": "uvwx2345-yz56-7890-abcd-efgh5678ijkl",
                                "sourceMetadataId": "metadata-3456-ghij-7890-klmn-1234opqrstuv",
                                "destinationMetadataId": "metadata-4567-ijkl-8901-mnop-2345qrstuvwx",
                                "sourceColumnName": "signup_date",
                                "destinationColumnName": "signup_date_formatted",
                                "sourceGroupId": "",
                                "destinationTaskId": "qrst8901-uv23-4567-wxyz-0123abcd4567",
                                "sourceTaskId": "",
                                "transformations": {
                                    "ids": ["DateFormatTransformation"]
                                }
                            }
                        ]
                    }
                ],
                "sequence": {
                    "d78gh90i-jk12-3456-lmno-7890pqrst123": []
                },
                "taskSequence": {
                    "mnop4567-ef89-0123-ghij-klmnopqr4567": ["qrst8901-uv23-4567-wxyz-0123abcd4567"]
                }
            },
            "source_connection_details": {
                "conn_id": "conn-mongo1-def2-ghi3-jkl4mnop5678",
                "conn_type": "MONGODB",
                "login": "mongoUser",
                "password": "mongoPass456",
                "host": "192.168.2.50",
                "port": 27017,
                "database": "customer_db"
            }
        }
    ]
      return few_shot_examples

    def construct_few_shot_text(self):
        examples_text = ""
        for example in self.few_shot_examples:
            for turn in example['dialogue']:
                examples_text += f"Assistant: {turn['bot']}\n"
                if 'user' in turn and turn['user']:
                    examples_text += f"User: {turn['user']}\n"
            examples_text += f"Assistant: Here is the Mapping JSON:\n{json.dumps(example['mapping_json'], indent=4)}\n\n"

            if 'source_connection_details' in example:
                examples_text += f"Assistant: And here are the Connection Details for Source:\n{json.dumps(example['source_connection_details'], indent=4)}\n\n"
            if 'dest_connection_details' in example:
                examples_text += f"Assistant: And here are the Connection Details for Destination:\n{json.dumps(example['dest_connection_details'], indent=4)}\n\n"
        return examples_text

    def get_initial_prompt(self):
            few_shot_text = self.construct_few_shot_text()
            initial_prompt = (
                "You are an AI assistant helping a user to generate JSON configurations for an ETL pipeline.\n\n"
                "Here are some examples:\n\n"
                f"{few_shot_text}"
                "Use the conversation history and the user's latest input to determine what information is missing. <<<do not ask multiple missing informations at once>>> <<ASK it part by part eg: source, destination, connection etc..>>>>"
                "Ask the user only for the information needed to generate the JSON configurations.\n"
                "Do not ask for IDs; instead, generate UUIDs by yourself when creating the JSONs.\n"
                "Make sure the Source connection ID match with mapping JSon's connection ID for source\n" 
                "Alo Make sure the Destination connection ID in destination connection datails match with mapping JSon's connection ID for destination\n"
                "- Keep your language non-technical and easy to understand, as users might not be familiar with technical terms.\n"
                "- Make sure to get these informations from user: details about source (sourceName, sourceType, tableName, sourceColumnName), destination (destinationName, destinationType, tableName, destinationColumnName), and connection details (login, password, host, port, database).\n"
                "Once all necessary information is collected, generate the Mapping JSON and the Connection Details for both the source and destination.\n"
                "ONLY generate all the JSONs once you have all the information for ALL THE JSONS.\n"
                "Specify them as follows:\n"
                "- 'Here is the Mapping JSON:'\n"
                "- 'And here are the Connection Details for Source:'\n"
                "- 'And here are the Connection Details for Destination:'\n"
                "Note:\n"
                "- Only generate the JSON configurations once you have all the information.\n"
                "-Only return the json nothing else\n"
                "-When all columns are mentioned keep the sourceColumnName and destinationColumnName as empty just like the way given the example above.\n"
            )
            return initial_prompt
    os.makedirs('/content/json_data', exist_ok=True)


    def generate_response(self, user_input):
        self.conversation_history.append({"role": "user", "content": user_input})

        messages = [{"role": "system", "content": self.initial_prompt}] + self.conversation_history

        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=messages,
            temperature=0.5,
            top_p=0.3,
        )
        assistant_reply = response['choices'][0]['message']['content']
        self.conversation_history.append({"role": "assistant", "content": assistant_reply})

        if "Here is the Mapping JSON:" in assistant_reply:
            # Extract and clean JSON configurations
            mapping_json_str, source_json_str, dest_json_str = self.extract_json_strings(assistant_reply)

            # Parse JSONs with error handling
            mapping_json, source_connection_details, dest_connection_details = self.parse_jsons(
                mapping_json_str, source_json_str, dest_json_str
            )


        return assistant_reply


    def extract_json_strings(self, assistant_reply):
        # Extracting JSON strings by markers
        mapping_json_str = assistant_reply.split("Here is the Mapping JSON:")[-1].split("And here are the Connection Details for Source:")[0].strip()
        source_json_str = assistant_reply.split("And here are the Connection Details for Source:")[-1].split("And here are the Connection Details for Destination:")[0].strip()
        dest_json_str = assistant_reply.split("And here are the Connection Details for Destination:")[-1].strip()

        # Removing formatting artifacts
        mapping_json_str = mapping_json_str.strip('```').strip().replace('\n', '').lstrip('json').strip()
        source_json_str = source_json_str.strip('```').strip().replace('\n', '').lstrip('json').strip()
        dest_json_str = dest_json_str.strip('```').strip().replace('\n', '').lstrip('json').strip()


        
        print("===========================")
        print("source_json_str:", source_json_str)
        print("===========================")

        print("dest_json_str:", dest_json_str)
        
        print("===========================")

        mapping_json_str=mapping_json_str.replace(r'\n', '')
        mapping_json_str=mapping_json_str.replace(r'\\', '')


        # Load the strings as JSON objects to avoid escaping issues
        mapping_json = json.loads(mapping_json_str)
        source_json = json.loads(source_json_str)
        dest_json = json.loads(dest_json_str)

        # Save the JSON objects to files without escaping
        self.save_json_and_download(mapping_json, 'C:\\EvanWorkSpace\\Evan\\Capstone\\file_transfer_airflow_local\\plugins\\mapping.json')
        self.save_json_and_download(source_json, 'C:\\EvanWorkSpace\\Evan\\Capstone\\file_transfer_airflow_local\\plugins\\source_connection_details.json')
        self.save_json_and_download(dest_json, 'C:\\EvanWorkSpace\\Evan\\Capstone\\file_transfer_airflow_local\\plugins\\dest_connection_details.json')




        return mapping_json_str, source_json_str, dest_json_str


    def parse_jsons(self, mapping_json_str, source_json_str, dest_json_str):
        # Parse JSON strings with error handling
        try:
            mapping_json = json.loads(mapping_json_str)
            source_connection_details = json.loads(source_json_str)
            dest_connection_details = json.loads(dest_json_str)
            return mapping_json, source_connection_details, dest_connection_details
        except json.JSONDecodeError as e:
            print("Error parsing JSON:", e)
            return None, None, None

    def save_json_and_download(self, data, filename):
        # Save and prepare JSON for download to local
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"\nThe JSON configuration '{filename}' has been saved and is ready for download.")


    def chat(self):
        print("Assistant: Hello! How can I help you today?")
        while True:
            user_input = input("User: ")
            if user_input.lower() in ['exit', 'quit']:
                print("Assistant: Goodbye!")
                break
            assistant_reply = self.generate_response(user_input)
            print(f"Assistant: {assistant_reply}")
            
if __name__ == "__main__":
    chatbot = DataMigrationChatbot()
    chatbot.chat()
