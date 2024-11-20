import sparknlp
from sparknlp.pretrained import PretrainedPipeline

spark = sparknlp.start(gpu=True)
pipeline = PretrainedPipeline("explain_document_dl_gpu", lang="en")
result = pipeline.annotate("John Snow Labs is amazing!")
print(result)