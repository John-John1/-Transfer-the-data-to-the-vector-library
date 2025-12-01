# Transfer the data to the vector library

#### 介绍
本项目是通过利用 Unstructured 库里的函数或者 api ，
把 PDF,PPTX,HTML 根据被解析为 elements 自动生成的元数据分块，
如果有自定义的元数据也会随后被自定义函数被插入，
被向量模型转化为向量后，
存入supabase(chroma DB)数据库，因为存入的数据含有元数据，
所以支持混合搜索

#### 软件架构
1. 前期准备工作
2. 将数据解析为 Element 格式，并临时转化为 JSON 展示
3. 自己定义一下自定义元信息框架
4. 取 element 的数据，然后创建了一个 Python 字典来保存映射关系
5. 把 key 和 vlaue 换一下位置（便于后续根据 parent_id 快速找到该元素属于哪个章节）
6. 把 PDF 中同一个章节下面的所有内容自动收集到一起，你的文档简单恢复结构
7. 根据解析过程中生成的元数据分块
8. 2.8 写入supabase或者内存类向量数据库（ChromaDB）


#### 使用说明
pip requiresments.txt
python main.py
