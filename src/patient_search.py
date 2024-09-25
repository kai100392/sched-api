from google.cloud import aiplatform
import vertexai
from vertexai.language_models import TextEmbeddingModel
import time
import math
# import tqdm  # to show a progress bar

# Load the text embeddings model
print("initializing pretrained model ...")
model = TextEmbeddingModel.from_pretrained("textembedding-gecko@001")
BATCH_SIZE = 5

def get_embeddings_wrapper(texts):
    print(texts)
    embs = []
    for i in range(math.ceil((len(texts) / BATCH_SIZE))):
        time.sleep(0.5)  # to avoid the quota error
        result = model.get_embeddings(texts[i : i + BATCH_SIZE])
        embs = embs + [e.values for e in result]
    return embs


def find_patients (
    VERTEX_PROJECT_ID: str,
    VERTEX_INDEX_ID:str,
    VERTEX_LOCATION: str,
    VERTEX_ENPOINT: str,
    pat: dict = {}):

    # init the vertexai package
    # print("initializing vertex ai project")
    # vertexai.init(project=VERTEX_PROJECT_ID, location=VERTEX_LOCATION)

    age = "The age of the patient is " + str(pat["PROSTATE_CANCER_VISIT_AGE_FIRST"]) + "." if pat["PROSTATE_CANCER_VISIT_AGE_FIRST"] != 0.0 else ""
    biopsy1 = " This patient has completed " + pat["biopsy_1"] + " procedure before " + str(pat["biopsy_1_days"]) + " days." if pat["biopsy_1"] != "" else " "
    biopsy1_abnormal = pat["biopsy_1"] + " is abnormal." if pat["biopsy_1_abnormal"] != "" else " "

    biopsy2 = " This patient has completed " + pat["biopsy_2"] + " procedure before " + str(pat["biopsy_2_days"]) + " days." if pat["biopsy_2"] != "" else " "
    biopsy2_abnormal = pat["biopsy_2"] + " is abnormal." if pat["biopsy_2_abnormal"] != "" else " "

    imaging1 = " This patient has completed " + pat["imaging_1"] + " procedure before " + str(pat["imaging_1_days"]) + " days." if pat["imaging_1"] != "" else " "
    imaging1_abnormal = pat["imaging_1"] + " is abnormal." if pat["imaging_1_abnormal"] != "" else " "

    imaging2 = " This patient has completed " + pat["imaging_2"] + " procedure before " + str(pat["imaging_2_days"]) + " days." if pat["imaging_2"] != "" else " "
    imaging2_abnormal = pat["imaging_2"] + " is abnormal." if pat["imaging_2_abnormal"] != "" else " "

    imaging3 = " This patient has completed " + pat["imaging_3"] + " procedure before " + str(pat["imaging_3_days"]) + " days." if pat["imaging_3"] != "" else " "
    imaging3_abnormal = pat["imaging_3"] + " is abnormal." if pat["imaging_3_abnormal"] != "" else " "

    psa1 = " This patient has completed " + pat["psa_1"] + " test " if pat["psa_1"] != "" else " "
    psa1_when = "before " + str(pat["psa_1_days"]) + " days." if pat["psa_1"] != "" else " "
    psa1_value = " The result of " + pat["psa_1"] + " test is " + str(pat["psa_1_value"]) + " " + pat["psa_1_unit"] + "." if pat["psa_1"] != "" else " "
    psa1_abnormal = pat["psa_1"] + " is abnormal." if pat["psa_1_abnormal"] != "" else " "

    psa2 = " This patient has completed " + pat["psa_2"] + " test " if pat["psa_2"] != "" else " "
    psa2_when = "before " + str(pat["psa_2_days.astype"]) + " days." if pat["psa_2"] != "" else " "
    psa2_value = " The result of " + pat["psa_2"] + " test is " + str(pat["psa_2_value"]) + " " + pat["psa_2_unit"] + "." if pat["psa_2"] != "" else " "
    psa2_abnormal = pat["psa_2"] + " is abnormal." if pat["psa_2_abnormal"] != "" else " "

    psa3 = " This patient has completed " + pat["psa_3"] + " test " if pat["psa_3"] != "" else " "
    psa3_when = "before " + str(pat["psa_3_days"]) + " days." if pat["psa_3"] != "" else " "
    psa3_value = " The result of " + pat["psa_3"] + " test is " + str(pat["psa_3_value"]) + " " + pat["psa_3_unit"] + "." if pat["psa_3"] != "" else " "
    psa3_abnormal = pat["psa_3"] + " is abnormal." if pat["psa_3_abnormal"] != "" else " "

    psa4 = " This patient has completed " + pat["psa_4"] + " test " if pat["psa_4"] != "" else " "
    psa4_when = "before " + str(pat["psa_4_days"]) + " days." if pat["psa_4"] != "" else " "
    psa4_value = " The result of " + pat["psa_4"] + " test is " + str(pat["psa_4_value"]) + " " + pat["psa_4_unit"] + "." if pat["psa_4"] != "" else " "
    psa4_abnormal = pat["psa_4"] + " is abnormal." if pat["psa_4_abnormal"] != "" else " "


    psa_recent = " Recent PSA increase percentage is " + str(pat["psa_recent_increase_percent"]) + " %." if pat["psa_recent_increase_percent"] != 0.0 else " "

    embedding = get_embeddings_wrapper([
        age + 
        biopsy1 + 
        biopsy1_abnormal + 
        biopsy2 + 
        biopsy2_abnormal + 
        imaging1 +
        imaging1_abnormal +
        imaging2 +
        imaging2_abnormal +
        imaging3 +
        imaging3_abnormal +
        psa1 +
        psa1_when +
        psa1_value + 
        psa1_abnormal +
        psa2 +
        psa2_when +
        psa2_value + 
        psa2_abnormal +
        psa3 +
        psa3_when +
        psa3_value + 
        psa3_abnormal +   
        psa4 +
        psa4_when +
        psa4_value + 
        psa4_abnormal +
        psa_recent])
    print(embedding)

    aiplatform.init(project=VERTEX_PROJECT_ID, location=VERTEX_LOCATION)

    # @param {type:"string"}
    my_index_endpoint = aiplatform.MatchingEngineIndexEndpoint(VERTEX_ENPOINT)

   # Test query
    response = my_index_endpoint.find_neighbors(
        deployed_index_id=VERTEX_INDEX_ID,
        queries=embedding,
        num_neighbors=5,
    )

    print(response[0])
    return response[0]
