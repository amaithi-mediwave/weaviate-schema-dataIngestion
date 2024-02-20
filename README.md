
# Weaviate Schema initialization, data ingestion and containeraization

### DataIngesetion 
The documents need to be stored inside the data folder
allowed file extensions are - ['.docx', '.pdf', '.txt' ]


```bash
  python dataIngestion.py --config=params.yaml
```

### Schema generation on weaviate
Creating Schema in the weaviate client

Schema design can be override.

```bash
  python schema.py --config=params.yaml
```
    
### Weaviate containeraization
Running weaviate on Container 

Docker compose can be customized using weaviate toolkit available in weaviate official website.
    
#### Services

- Weaviate local instance
- text-spellcheck
- t2v-gpt4all - Embedding model
- redis-memory - for Redis message Store and  Semantic cache

```bash
  sudo docker-compose up
```

