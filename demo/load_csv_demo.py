from pystellardb import stellar_hive

"""
通过python脚本导入csv文件的demo

StellarDB的导入流程为：
1.原始数据csv文件上传到HDFS
2.通过sql建外表，把csv文件转换成sql表
3.建图（需要指定图schema）
4.从外表bulkload导入图

这里提供2-3-4的demo
为了使用方便，提供了将2、3、4步对应的python function，把schema从python格式转化成sql/cypher语句并执行
按main中的示例调用即可
"""


# 注，当前demo还不支持自动去除csv文件的首行，上传hdfs之前需要手动去除一下
def create_table(cursor, hdfs_path, tbl_name, tbl_schema, stored_as="textfile", delimiter=","):
    """
    :param cursor: stellar_hive.StellarCursor
    :param hdfs_path: 原始数据文件的hdfs目录
    :param tbl_name: 建表的名称
    :param tbl_schema: 包含csv文件里每个字段的(名称,类型)元组，如：tbl_schema = [("name", "string"), ("age", "int")]
    :param stored_as: 存储格式，可选[csvfile, orc, textfile]等
    :param delimiter: 原始数据文件的分隔符
    :return:
    """
    sql1 = "create external table if not exists {} (".format(tbl_name)
    sql2 = ""
    for item in tbl_schema:
        tmp = item[0] + " " + item[1] + ",\n"
        sql2 += tmp
    sql2 = sql2[:-2]  # 去除最后多余的换行符和逗号
    sql3 = ") row format delimited fields terminated by '{}'".format(delimiter)
    sql4 = "stored as {}".format(stored_as)
    sql5 = "location '{}'".format(hdfs_path)

    sql = sql1 + "\n" + sql2 + "\n" + sql3 + "\n" + sql4 + "\n" + sql5

    # execute
    cursor.execute("config query.lang sql")
    print("Executing sql:")
    print(sql + "\n")
    cursor.execute(sql)


def create_graph(cursor, graph_name, vertex_dict, edge_dict, shard_num, replication_num=3):
    """
    建图时，vertex_dict中， uid和ulabel为默认字段，不需要在属性元组中指定
    同理，edge_dict中，usid, uslable, udid, udlabel, uelabel都不需要在属性元组中指定

    :param cursor: stellar_hive.StellarCursor
    :param graph_name: 图名
    :param vertex_dict: 字典，key为label，value为这个label下的属性(名称，类型)元组
    :param edge_dict: 字典，key为label，value为这个label下的属性(名称，类型)元组
    :param shard_num: 建图时指定图的shard数，shard数量和图处理时的并行度有关
    :param replication_num: 建图时指定图的副本数
    :return:
    """
    cypher1 = "create graph {} with schema ".format(graph_name)
    cypher_vertex = ""
    for vertex_label in vertex_dict:
        cypher_vertex_for_label = "(:{} ".format(vertex_label)
        cypher_vertex_for_label += "{"
        v_label_schema = vertex_dict[vertex_label]
        for item in v_label_schema:
            tmp = item[0] + " " + item[1] + ", "
            cypher_vertex_for_label += tmp
        if len(v_label_schema) == 0:
            cypher_vertex_for_label = cypher_vertex_for_label[:-2] + ") "
        else:
            cypher_vertex_for_label = cypher_vertex_for_label[:-2] + "}) "
        cypher_vertex += cypher_vertex_for_label
    cypher_edge = ""
    for edge_label in edge_dict:
        cypher_edge_for_label = "[:{} ".format(edge_label)
        cypher_edge_for_label += "{"
        e_label_schema = edge_dict[edge_label]
        for item in e_label_schema:
            tmp = item[0] + " " + item[1] + ", "
            cypher_edge_for_label += tmp
        if len(e_label_schema) == 0:
            cypher_edge_for_label = cypher_edge_for_label[:-2] + "] "
        else:
            cypher_edge_for_label = cypher_edge_for_label[:-2] + "}] "
        cypher_edge += cypher_edge_for_label
    cypher2 = "graphproperties:{`graph.shard.number`:" + str(shard_num) + \
              " ,`graph.replication.number`:" + str(replication_num) + "}"
    cypher = cypher1 + cypher_vertex + cypher_edge + cypher2

    # execute
    cursor.execute("config query.lang cypher")
    print("Executing cypher:")
    print(cypher + "\n")
    cursor.execute(cypher)


def load_vertex_from_table_into_graph(cursor, tbl_name, graph_name, label_name, load_schema):
    """
    :param cursor: stellar_hive.StellarCursor
    :param tbl_name: table name
    :param graph_name: graph name
    :param label_name: label名，由于这里是单张表load，一次只能load一个label的数据
    :param load_schema: 指定图<-表的字段对应关系，如[("__uid", "uid"), ("name", "name")]。第一个元组第一个元素必须是"__uid"
    :return:
    """
    cypher1 = "select * from default.{} bulk upsert (:{}".format(tbl_name, label_name)
    cypher1 += " {"
    if load_schema[0][0] != "__uid":
        raise ValueError("load_schema第一项必须指定__uid!")
    for item in load_schema:
        tmp = item[0] + ":" + item[1] + ", "
        cypher1 += tmp
    cypher = cypher1[:-2] + "})"

    # execute
    cursor.execute("config query.lang cypher")
    cursor.execute("config crux.execution.mode analysis")
    cursor.execute("use graph {}".format(graph_name))
    print("Executing cypher:")
    print(cypher)
    cursor.execute(cypher)
    print("Result:")
    print(cursor.fetchall())
    print(cypher + "\n")


def load_edge_from_table_into_graph(cursor, tbl_name, graph_name, label_name, load_schema):
    """
    :param cursor: stellar_hive.StellarCursor
    :param tbl_name: table name
    :param graph_name: graph name
    :param label_name: label名，由于这里是单张表load，一次只能load一个label的数据
    :param load_schema: 指定图<-表的字段对应关系，如[("__usid", "usid"), ("__udid", "udid"),
    ("__uslabels", "[uslabel]"), ("__udlabels": "[udlabel]")]。
    前4个元组第一个元素必须依次是"__usid", "__udid", "__uslabels", "__udlabels"
    注，当csv文件/外表里没有起点、终点label对应字段时（假设我们知道起点、终点label都是"vertex"）
    可以指定("__uslabels", "['vertex']"), ("__udlabels": "['vertex']"), 多加一对引号
    这样，导入时会把所有的起点、终点label都打成"vertex"

    :return:
    """
    cypher1 = "select * from default.{} bulk upsert [:{}".format(tbl_name, label_name)
    cypher1 += " {"
    if len(load_schema) < 4:
        raise ValueError("load_schema缺少数据！请重新检查")
    condition = load_schema[0][0] == "__usid" and load_schema[1][0] == "__udid" and \
                load_schema[2][0] == "__uslabels" and load_schema[3][0] == "__udlabels"
    if not condition:
        raise ValueError("边的load_schema前4项必须依次指定__usid, __udid, __uslabels, __udlabels!")
    for item in load_schema:
        tmp = item[0] + ":" + item[1] + ", "
        cypher1 += tmp
    cypher = cypher1[:-2] + "}]"

    # execute
    cursor.execute("config query.lang cypher")
    cursor.execute("config crux.execution.mode analysis")
    cursor.execute("use graph {}".format(graph_name))
    print("Executing cypher:")
    print(cypher)
    cursor.execute(cypher)
    print("Result:")
    print(cursor.fetchall())
    print(cypher + "\n")


# demo
if __name__ == '__main__':
    # 创建jdbc连接
    conn = stellar_hive.StellarConnection(host="host1",
                                          port=10000,
                                          graph_name='pokemon',  # graph_name先随便填一个
                                          auth="KERBEROS",
                                          database="default",
                                          kerberos_service_name='hive')
    cur = conn.cursor()

    # step 1: 创建点/边外表
    hdfs_path_vertex = "/user/loading/graph/v/"  # 预先把点.csv传到hdfs的这个路径下，请先手动去掉csv文件首行
    hdfs_path_edge = "/user/loading/graph/e/"  # 预先把边.csv传到hdfs的这个路径下，请先手动去掉csv文件首行
    tbl_name_vertex = "vm_node"
    tbl_name_edge = "ker_mod_container"
    # 指定点、边外表的schema，对应csv文件中每个字段和字段类型
    tbl_schema_vertex = [("uid", "string"), ("ulabel", "string"), ("name", "string"), ("author", "string"),
                         ("license", "string"), ("srcversion", "string"), ("depends", "string"), ("intree", "string"),
                         ("parm", "string"), ("filename", "string"), ("kernelname", "string")]
    tbl_schema_edge = [("usid", "string"), ("uslabel", "string"), ("udid", "string"), ("udlabel", "string"),
                       ("uelabel", "string")]

    # 根据上面的信息，从点、边csv文件创建点、边外表
    create_table(cur, hdfs_path_vertex, tbl_name_vertex, tbl_schema_vertex)
    create_table(cur, hdfs_path_edge, tbl_name_edge, tbl_schema_edge)

    # step2: 指定图schema，创建图
    graph_name = "vm_graph"
    vertex_dict = dict()  # 用来指定图的vertex schema，每个key是一个label，value用来指定这个label下的属性
    edge_dict = dict()  # 用来指定图的edge schema，每个key是一个label，value用来指定这个label下的属性
    # 建图时，vertex_dict中， uid和ulabel为默认字段，不需要在属性元组中指定
    # 同理，edge_dict中，usid, uslable, udid, udlabel, uelabel都不需要在属性元组中指定
    vertex_dict["Module"] = [("name", "string"), ("filename", "string"), ("kernelname", "string")]
    edge_dict["Container"] = []
    shard_num = 3
    # 根据上面的信息创建图
    create_graph(cur, graph_name, vertex_dict, edge_dict, shard_num)

    # step3: 从外表把数据导入图
    # 首先指定点、边的load_schema，也就是(图中的字段名, 表中的字段名)
    # 点的load_schema第一项必须指定__uid
    load_schema_vertex = [("__uid", "uid"), ("name", "name"), ("filename", "filename"), ("kernelname", "kernelname")]
    # 边的load_schema前4项必须依次指定__usid, __udid, __uslabels, __udlabels
    load_schema_edge = [("__usid", "usid"), ("__udid", "udid"), ("__uslabels", "[uslabel]"), ("__udlabels", "[udlabel]")]
    """
    注:
    当csv文件/外表里没有起点、终点label对应字段时（假设我们知道起点、终点label都是"vertex"）
    可以指定("__uslabels", "['vertex']"), ("__udlabels": "['vertex']") 多加一对引号
    这样，导入时会把所有的起点、终点label都打成"vertex"
    """
    label_name_vertex = "Module"
    label_name_edge = "Container"
    # 导入点、导入边
    load_vertex_from_table_into_graph(cur, tbl_name_vertex, graph_name, label_name_vertex, load_schema_vertex)
    load_edge_from_table_into_graph(cur, tbl_name_edge, graph_name, label_name_edge, load_schema_edge)

    # 删除图
    cur.execute("config query.lang cypher")
    cur.execute("drop graph {}".format(graph_name))
