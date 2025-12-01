import warnings
warnings.filterwarnings("ignore")

import os
import json
import requests
import psycopg2

from unstructured.partition.pdf import partition_pdf
from unstructured.partition.html import partition_html
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.md import partition_md
from unstructured.chunking.title import chunk_by_title
from unstructured.staging.base import dict_to_elements

from langchain_core.documents import Document
from langchain.embeddings.base import Embeddings
# Baidu Embedding API

class BaiduEmbeddings(Embeddings):
    def __init__(self):
        self.API_KEY = "bce-v3/ALTAK-GGlAZiiVpbSzn1mZZkl0U/8d2c1619ccfb488a569efedaa257f9e42aa74b83"
        self.url = "https://qianfan.baidubce.com/v2/embeddings"

    def embed_query(self, text: str):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.API_KEY}",
        }
        payload = {"input": text}

        res = requests.post(self.url, headers=headers, json=payload).json()
        if "data" in res and len(res["data"]) > 0:
            return res["data"][0]["embedding"]

        # 新模型返回：{"result":{"embedding":[...]}}
        if "result" in res and "embedding" in res["result"]:
            return res["result"]["embedding"]

        print("❌ 百度 Embedding 返回异常：", res)
        return [0.0] * 768  # 保底不报错

    def embed_documents(self, texts):
        return [self.embed_query(t) for t in texts]


embeddings = BaiduEmbeddings()
# Unstructured Strong API
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared

s = UnstructuredClient(
    api_key_auth="BAjhezCSAMEhJ9CrvLvJTVlbZjoT30",
    server_url="https://api.unstructured.io"
)


def parse_pdf_via_api(filepath):
    print("→ Strong API 解析 PDF:", filepath)

    with open(filepath, "rb") as f:
        files = shared.Files(
            content=f.read(),
            file_name=os.path.basename(filepath),
        )

    req = shared.PartitionParameters(
        files=files,
        strategy="hi_res",
        hi_res_model_name="yolox",
        pdf_infer_table_structure=True,
        skip_infer_table_types=[],
    )

    resp = s.general.partition(req)
    return dict_to_elements(resp.elements)

# 对这几种文件进行解析
def parse_file(filepath):
    ext = filepath.lower().split(".")[-1]

    if ext == "html":
        return partition_html(filename=filepath)
    elif ext == "pptx":
        return partition_pptx(filename=filepath)
    elif ext == "md":
        return partition_md(filename=filepath)
    elif ext == "pdf":
        try:
            return parse_pdf_via_api(filepath)
        except Exception as e:
            print("Strong API failed:", e)
            return []
    else:
        print("Unsupported file:", filepath)
        return []
def load_all_elements_from_folder(folder):
    all_elements = []
    for fname in os.listdir(folder):
        path = os.path.join(folder, fname)
        if os.path.isfile(path):
            print("解析文件:", path)
            els = parse_file(path)
            all_elements.extend(els)
    return all_elements


def chunk_elements(elements):
    return chunk_by_title(
        elements,
        combine_text_under_n_chars=100,
        max_characters=3000,
    )


def convert_chunks_to_documents(chunks):
    docs = []
    for c in chunks:
        meta = c.metadata.to_dict()
        meta["source"] = meta.get("filename", "")
        docs.append(Document(page_content=c.text, metadata=meta))
    return docs

# pgvector
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="postgres",
    user="postgres",
    password="zydsslxd"
)
cur = conn.cursor()


def write_documents_to_supabase(documents):
    sql = """
        INSERT INTO documents (content, metadata, embedding)
        VALUES (%s, %s, %s)
    """

    for doc in documents:
        txt = doc.page_content
        meta_json = json.dumps(doc.metadata)

        vector = embeddings.embed_query(txt)
        vector_str = "[" + ",".join(str(v) for v in vector) + "]"

        cur.execute(sql, (txt, meta_json, vector_str))

    conn.commit()
    print("→ Insert completed")

# 主流程
def ingest_folder_into_supabase(folder="E:\documents"):
    print("=== Load elements ===")
    elements = load_all_elements_from_folder(folder)

    print("=== Chunk ===")
    chunks = chunk_elements(elements)

    print("=== Build documents ===")
    docs = convert_chunks_to_documents(chunks)

    print("=== Insert into DB ===")
    write_documents_to_supabase(docs)

    print("=== Done ===")


# 入口
ingest_folder_into_supabase("E:\documents")
