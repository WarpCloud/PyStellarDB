from pystellardb import stellar_hive


def pagerank(cursor,
             graph_name,
             damping_factor=0.85,
             rounds=10,
             limit=None,
             view_path='/tmp/algo_test/centrality/ppr',
             reuseGraph=True):
    algo_cypher1 = "create query temporary graph view algo_test as (a) [b] with graph_pagerank" \
                   "(@algo_test, '{}', ".format(view_path)
    algo_cypher1 += " '{"
    algo_cypher2 = "reuseGraph:{}, factor:{}, rounds:{}".format(str(reuseGraph).lower(), damping_factor, rounds)
    algo_cypher2 += "}') as unapply(vertex, rank) \n"
    if limit is None:
        algo_cypher3 = "return node_rk_to_uid(vertex) as vid, rank order by rank desc"
    elif isinstance(limit, (int, str)):
        algo_cypher3 = "return node_rk_to_uid(vertex) as vid, rank order by rank desc limit {}".format(limit)
    else:
        raise TypeError("参数limit必须是int或str类型！")

    algo_cypher = algo_cypher1 + algo_cypher2 + algo_cypher3

    cursor.execute('config query.lang cypher')
    cursor.execute('config crux.execution.mode analysis')
    cursor.execute('use graph {}'.format(graph_name))
    print("Executing cypher:")
    print(algo_cypher + "\n")
    cursor.execute(algo_cypher)
    query_results = cursor.fetchall()
    results = []
    for j, query_result in enumerate(query_results):
        vid = query_result[0]
        pr = query_result[1]
        results.append((vid, pr))
    return results


def personalized_pagerank(cursor,
                          graph_name,
                          personalization,
                          damping_factor=0.85,
                          rounds=10,
                          limit=None,
                          view_path='/tmp/algo_test/centrality/ppr/',
                          reuseGraph=True):

    decl_cypher1 = "decl sourceNodes:list[bytes]"
    decl_cypher2 = "sourceNodes:= match (a) where uid(a) in {} return collect(id(a))" .format(str(personalization))

    algo_cypher1 = "create query temporary graph view algo_test as (a) [b] with graph_personalized_pagerank" \
                   "(@algo_test, '{}', @sourceNodes, ".format(view_path)
    algo_cypher1 += " '{"
    algo_cypher2 = "reuseGraph:{}, factor:{}, rounds:{}".format(str(reuseGraph).lower(), damping_factor, rounds)
    algo_cypher2 += "}') as unapply(vertex, rank) \n"
    if limit is None:
        algo_cypher3 = "return node_rk_to_uid(vertex) as vid, rank order by rank desc"
    elif isinstance(limit, (int, str)):
        algo_cypher3 = "return node_rk_to_uid(vertex) as vid, rank order by rank desc limit {}".format(limit)
    else:
        raise TypeError("参数limit必须是int或str类型！")

    algo_cypher = algo_cypher1 + algo_cypher2 + algo_cypher3

    cursor.execute('config query.lang cypher')
    cursor.execute('config crux.execution.mode analysis')
    cursor.execute('use graph {}'.format(graph_name))
    cursor.execute(decl_cypher1)
    cursor.execute(decl_cypher2)
    print("Executing cypher:")
    print(algo_cypher + "\n")
    cursor.execute(algo_cypher)
    query_results = cursor.fetchall()
    results = []
    for j, query_result in enumerate(query_results):
        vid = query_result[0]
        pr = query_result[1]
        results.append((vid, pr))
    return results


conn = stellar_hive.StellarConnection(host="host1",
                                      port=10000,
                                      graph_name='pokemon',
                                      auth="KERBEROS",
                                      kerberos_service_name='hive')

cur = conn.cursor()
result1 = pagerank(cur, "family", reuseGraph=False)
result2 = personalized_pagerank(cur, "family", ["1", "2"])

print("done")
