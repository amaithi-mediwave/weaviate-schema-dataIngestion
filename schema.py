import weaviate
import os 
import yaml 
import argparse 


#------------------------------------------------------------------

def schema_generation(config_path):
  
  config = read_params(config_path)

  weaviate_url = config['dataIngestion']['weaviate']['weaviate_url']

  weaviate_class = config['dataIngestion']['weaviate']['class_name']

  weaviate_class_property = config['dataIngestion']['weaviate']['property']

   

  client = weaviate.Client(
    url=weaviate_url,
  )


  if (client.schema.exists(weaviate_class)):
      print("Class exist")
      client.schema.delete_all()
      print("The Existing Schema is deleted")
  else:
      print("class does not exist")


  schema = {
      "classes": [
          {
              "class": weaviate_class,
              "description": "data",
          "moduleConfig": {
          "text2vec-gpt4all": {
            "model": "sentence-transformers/all-MiniLM-L6-v2",
            "options": {
              "waitForModel": True,
              "useGPU": True,
              "useCache": True
              }
            }
          },
              "properties": [
                  {
                      "dataType": ["text"],
                      "description": "The content of the paragraph",
                      "moduleConfig": {
                          "text2vec-gpt4all": {
                            "skip": False,
                            "vectorizePropertyName": False
                          }
                        },
                      "name": weaviate_class_property,
                  },
              ],
          "vectorizer":"text2vec-gpt4all"
          },
      ]
  }

  client.schema.create(schema)
  if (client.schema.exists(weaviate_class)):
      print("Schema created")
   
#------------------------------------------------------------------
   
  
def read_params(config_path):
  '''
  Get the Params from local directory
  '''
  with open(config_path) as yaml_file:
      config = yaml.safe_load(yaml_file)
  
  return config

#------------------------------------------------------------------





if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    schema_generation(parsed_args.config)