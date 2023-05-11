import json
from pystellardb import stellar_hive
from pystellardb.graph_types import Vertex, Edge

# change host & graph_name
conn = stellar_hive.StellarConnection(host="host1", port=10000, graph_name='family')
cur = conn.cursor()
cur.execute('config query.lang cypher')
cur.execute('use graph family')
cur.execute('match p = (a)-[f]->(b) return a,f,b limit 5')

query_results = cur.fetchall()

# 由于我们limit5，query_results中一共会有5个result
# 每个result是一个tuple，tuple中的每个元素对应cypher语句中return的元素
# 比如这里return a,f,b，每个result tuple中就会有3个元素，类型分别是Vertex, Edge, Vertex对象
for result in query_results:
    a = result[0]  # Vertex对象
    f = result[1]  # Edge对象
    b = result[2]  # Vertex对象

    # Vertex对象提供的方法如下：
    a_userid = a.getUserId()
    a_label = a.getLabel()
    a_fields = a.getFields()  # 查看a有哪些fields(属性)
    a_field1_value = a.getField("age")  # 取a的age属性的值

    # Edge对象提供的方法如下：
    f_uid = f.getUid()  # 边的uid
    f_label = f.getLabel()
    f_srcNode = f.getStartNode()  # 边的起点，得到的是一个没有fields的Vertex对象，可以获取起点的userId和label
    f_endNode = f.getEndNode()  # 边的终点，得到的是一个没有fields的Vertex对象，可以获取终点的userId和label
    f_fields = f.getFields()  # 查看f有哪些fields(属性)
    f_field1_value = a.getField("createTime")  # 取f的createTime属性的值

    # Vertex和Edge类提供toJSON方法，可以把Vertex和Edge对象转成类json格式的字典
    # 用json.dumps()进一步转成json字符串
    c = json.dumps(Vertex.toJSON(a))
    d = json.dumps(Edge.toJSON(f))
    e = json.dumps(Vertex.toJSON(b))




