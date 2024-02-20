from langchain_community.document_loaders import UnstructuredFileLoader
from unstructured.cleaners.core import clean_extra_whitespace
from langchain_community.document_loaders import WebBaseLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import WebBaseLoader

import weaviate
import time
import os
import argparse 
import yaml 

#------------------------------------------------------------------

def document_loader(config_path):

  config = read_params(config_path)

  website_url = config['dataIngestion']['website']

  chunk_size = config['dataIngestion']['chunk_size']

  chunk_overlap = config['dataIngestion']['chunk_overlap']

  data_dir = config['dataIngestion']['data']['directory_path']

  file_extensions = config['dataIngestion']['file_extensions']

  weaviate_url = config['dataIngestion']['weaviate']['weaviate_url']

  weaviate_class = config['dataIngestion']['weaviate']['class_name']

  weaviate_class_property = config['dataIngestion']['weaviate']['property']

  weaviate_batch_size = config['dataIngestion']['batch_config']['batch_size']

  weaviate_batch_dynamic = config['dataIngestion']['batch_config']['dynamic']

  weaviate_batch_timeout_retry = config['dataIngestion']['batch_config']['timeout_retries']





  # files_path = [os.path.abspath(x) for x in os.listdir(f"{data_dir}") if x.endswith(tuple(file_extensions))]

  dir = os.getcwd()+data_dir

  # files_path = [os.path.abspath(x) for x in os.listdir(f"{data_dir}") if x.endswith(tuple(file_extensions))]
  files_path = [dir+x for x in os.listdir(f"{dir}") if x.endswith(tuple(file_extensions))]

  print(files_path)
  client = weaviate.Client(weaviate_url)

  client.batch.configure(
  # `batch_size` takes an `int` value to enable auto-batching
  # (`None` is used for manual batching)
  batch_size=weaviate_batch_size,
  # dynamically update the `batch_size` based on import speed
  dynamic=weaviate_batch_dynamic,
  # `timeout_retries` takes an `int` value to retry on time outs
  timeout_retries=weaviate_batch_timeout_retry,
  # checks for batch-item creation errors
  # this is the default in weaviate-client >= 3.6.0
  callback=check_batch_result,
  
  # consistency_level=weaviate.data.replication.ConsistencyLevel.ALL,  # default QUORUM
)


  if website_url != '':
    print("\n\n-------------------------------\n\n")
    print(f"\t\t Processing Website : {website_url}", end="\n\n")

    loader = WebBaseLoader(website_url)
    data = loader.load()
    docs_dict = text_splitter(data, chunk_size, chunk_overlap, weaviate_class_property)
    ingestion(docs_dict, client, weaviate_class)

    print(f"\n\t\t {website_url} Ingestion completed Successfully", end="\n\n")



  for file in files_path:
     print("\n\n-------------------------------\n\n")
     print(f"\t\t Processing : {os.path.basename(file)}", end="\n\n")

     loader = UnstructuredFileLoader(file, 
                                # mode='elements',
                               post_processors=[clean_extra_whitespace],
                                )
     data = loader.load()
     docs_dict = text_splitter(data, chunk_size, chunk_overlap, weaviate_class_property)
     ingestion(docs_dict, client, weaviate_class)

     print(f"\t\t {os.path.basename(file)} Ingestion Completed Successfully", end="\n\n")

  count = client.query.aggregate(weaviate_class).with_meta_count().do()

  vector_count = count['data']['Aggregate']['GRP'][0]['meta']['count']

  print(f"\t\t Total Vectors in the Collection -> {weaviate_class} : {vector_count} ", end="\n\n")
#------------------------------------------------------------------



def ingestion(docs_dict, client, classname):

  with client.batch as batch:
      # for data_object in data_objects:
    # for i, data in enumerate(elements):  
    for doc in docs_dict:
        
      print(f"processing batch {batch}")

      # properties = {
      #     "content": data,
      # }
      time.sleep(1)

      batch.add_data_object(doc, class_name=classname)


def text_splitter(data, chunk_size, chunk_overlap, weaviate_class_property):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    all_splits = text_splitter.split_documents(data)

    docs_dict = [{f"{weaviate_class_property}": doc.page_content, "metadata": doc.metadata} for doc in all_splits]
    
    return docs_dict
     

def check_batch_result(results: dict):
  """
  Check batch results for errors.

  Parameters
  ----------
  results : dict
      The Weaviate batch creation return value, i.e. returned value of the client.batch.create_objects().
  """
  if results is not None:
    for result in results:
      if 'result' in result and 'errors' in result['result']:
        if 'error' in result['result']['errors']:
          print("We got an error!", result)
          break



def read_params(config_path):
    '''
    Get the Params from local directory
    '''
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    
    return config




if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    document_loader(parsed_args.config)