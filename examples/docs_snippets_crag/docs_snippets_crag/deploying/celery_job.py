from dagster import fs_io_manager, graph, op
from dagster_celery import celery_executor


@op
def not_much():
    return


@graph
def parallel_graph():
    for i in range(50):
        not_much.alias("not_much_" + str(i))()


celery_job = parallel_graph.to_job(
    resource_defs={"io_manager": fs_io_manager},
    executor_def=celery_executor,
)