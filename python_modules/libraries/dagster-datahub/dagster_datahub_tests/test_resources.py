from unittest import mock

import pytest
import responses

from datahub.configuration.common import ConfigurationError
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.kafka_emitter import MCE_KEY
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass

from dagster import (
    op,
    build_op_context,
    OpExecutionContext,
    DagsterResourceFunctionError,
    DagsterInvalidConfigError,
)
from dagster_datahub import datahub_kafka_emitter, datahub_rest_emitter


@responses.activate
def test_datahub_rest_emitter_resource():
    @op(required_resource_keys={"datahub"})
    def datahub_op(context: OpExecutionContext):
        assert context.resources.datahub is not None
        dataset_properties = DatasetPropertiesClass(
            description="This table stored the canonical User profile",
            customProperties={"governance": "ENABLED"},
        )

        # Construct a MetadataChangeProposalWrapper object.
        metadata_event = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=builder.make_dataset_urn("bigquery", "my-project.my-dataset.user-table"),
            aspectName="datasetProperties",
            aspect=dataset_properties,
        )
        context.resources.datahub.emit(metadata_event)

    with responses.RequestsMock() as rsps:
        # Creating the datahub_rest_emitter resource will run the DatahubRestEmitter.test_connection() method.
        # The response needs to be mocked out prior to the resource gets instantiated.
        rsps.add(rsps.GET, "http://localhost:8080/config", status=200, json={"noCode": "true"})
        rsps.add(
            rsps.POST, "http://localhost:8080/aspects?action=ingestProposal", status=200, json={}
        )
        context = build_op_context(resources={"datahub": datahub_rest_emitter})
        datahub_op(context)


def test_datahub_emitter_resource_failure():
    """Assert creating the resource without the mocked response fails to construct.
    DatahubRestEmitter.test_connection will fail due to the lack of a mock"""
    with pytest.raises(DagsterResourceFunctionError):

        @op(required_resource_keys={"datahub"})
        def bad_datahub_op():
            # Error should be thrown prior to executing the op.
            # Should Fail if it gets to this point
            assert False

        context = build_op_context(
            resources={
                "datahub": datahub_rest_emitter.configured(
                    {"read_timeout_sec": 0.01, "connect_timeout_sec": 0.01, "retry_max_times": 1}
                )
            }
        )
        bad_datahub_op(context)


def test_datahub_kafka_emitter_resource():
    @op(required_resource_keys={"datahub"})
    def datahub_op(context: OpExecutionContext):
        assert context.resources.datahub is not None

    context = build_op_context(
        resources={
            "datahub": datahub_kafka_emitter.configured(
                {"connection": {"bootstrap": "foobar:9092"}}
            )
        }
    )
    datahub_op(context)


def test_datahub_kafka_emitter_resource_failure():
    @op(required_resource_keys={"datahub"})
    def datahub_op(context: OpExecutionContext):
        assert context.resources.datahub is not None

    with pytest.raises((ConfigurationError, DagsterResourceFunctionError)):
        context = build_op_context(
            resources={
                "datahub": datahub_kafka_emitter.configured(
                    {
                        "connection": {"bootstrap": "foobar:9092"},
                        "topic": "NewTopic",
                        "topic_routes": {MCE_KEY: "NewTopic"},
                    }
                )
            }
        )
        datahub_op(context)
