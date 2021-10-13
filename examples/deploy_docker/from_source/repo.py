import time

from dagster import graph, op, repository, schedule


@op
def hello():
    return 1


@op
def hanging_solid():
    while True:
        time.sleep(5)


@graph
def hanging_graph():
    hanging_solid()


@graph
def my_graph():
    hello()


my_job = my_graph.to_job()


@schedule(cron_schedule="* * * * *", job=my_job, execution_timezone="US/Central")
def my_schedule(_context):
    return {}


@repository
def deploy_docker_repository():
    return [my_job, hanging_graph.to_job(), my_schedule]
