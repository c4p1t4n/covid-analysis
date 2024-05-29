from dataclasses import dataclass
from constructs import Construct
from aws_cdk import RemovalPolicy
from aws_cdk import (
    Tags,
    Stack,
    Duration,
    aws_ecr as ecr,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_events_targets as targets,
    aws_events as events,
    aws_lambda as _lambda,
    aws_sns_subscriptions as sns_subscriptions,
    aws_s3 as s3,
    aws_sns as sns
)

def template_sns_topic(scope:Construct,id:str,name:str) -> sns.Topic:
    return sns.Topic(scope,topic_name=name,id=id)

def template_role_lambda(scope: Construct, id_name: str, role_name: str) -> iam.Policy:
    """
    Parameters:
        scope: O escopo no qual a role deve ser criado.(self)
        id_name: O nome do ID da role.
        role_name: O nome da role.
    Returns
        Uma policy  IAM que pode ser usada por funções Lambda.
"""


    return iam.Role(
            scope=scope,
            id=id_name,
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            role_name=role_name
        )


def template_iam(actions: list, resources: list) -> iam.PolicyStatement:
    """
    Return a Policy Statement.

    Parameters:
        actions (list): A list of permissions to be applied.
        resources (list): A list of resources to which the actions will apply.
    Returns:
        iam.PolicyStatement: A policy statement defining permissions and resources.
    """

    return iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=actions,
        resources=resources
    )


def template_policy(scope: Construct, id_name: str, statements: list) -> iam.Policy:
    """

    Args:
        scope: self
        id_name: policy_test
        statements: [iam.PolicyStatement(args)] or template_iam_principal(args)]
    """
    return iam.Policy(
        scope=scope,
        id=id_name,
        statements=statements
    )

def template_lambda(
    scope: Construct,
    iam_role: iam,
    lambda_handler: str,
    lambda_name: str,
    code_path: str,
    id_name: str,
    minutes: int
) -> _lambda.Function:
    """
    Creates a Lambda function
    Parameters:
        scope (Construct):  O escopo no qual a lambda deve ser criado.(self)
        iam_role (iam): a Role  de permissoes da lambda
        lambda_handler (str): The Lambda handler function.
        lambda_name (str): The name of the Lambda function.
        code_path (str): O caminho para o codigo da lambda function
        id_name (str): o id da lambda
        minutes (int): Tempo de timeout da lambda

    Returns:
        lambda.Function

        
    """

    return _lambda.Function(
        scope=scope,
        memory_size=512,
        role=iam_role,
        timeout=Duration.minutes(minutes),
        handler=lambda_handler,
        runtime=_lambda.Runtime.PYTHON_3_10,
        id=id_name,
        function_name=lambda_name,
        code=_lambda.Code.from_asset(code_path),
        layers=[_lambda.LayerVersion.from_layer_version_arn(
            scope=scope
            ,id='layer_wrangler_pandas'
            ,layer_version_arn='arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python310:15')]
    )


def template_lambda_none(
    scope: Construct,
    iam_role: iam,
    lambda_name: str,
    id_name: str,
    minutes: int
) -> _lambda.Function:
    """
    Creates a Lambda function
    Parameters:
        scope (Construct):  O escopo no qual a lambda deve ser criado.(self)
        iam_role (iam): a Role  de permissoes da lambda
        lambda_handler (str): The Lambda handler function.
        lambda_name (str): The name of the Lambda function.
        code_path (str): O caminho para o codigo da lambda function
        id_name (str): o id da lambda
        minutes (int): Tempo de timeout da lambda

    Returns:
        lambda.Function

        
    """

    return _lambda.Function(
        scope=scope,
        memory_size=256,
        role=iam_role,
        timeout=Duration.minutes(minutes),
        runtime=_lambda.Runtime.PYTHON_3_10,
        handler="index.handler",
        id=id_name,
        function_name=lambda_name,
        code=_lambda.Code.from_inline("def handler(event, context): pass")

    )



def template_sqs_queue(scope:Construct,id_str:str,queue_name:str,delay_seconds:int=0,visibility_timeout:int=30):
    return sqs.Queue(scope,id=id_str,queue_name=queue_name,removal_policy=RemovalPolicy.DESTROY,visibility_timeout=Duration.seconds(visibility_timeout))




def template_iam_principal(actions: list, resources: list, principals: list) -> iam.PolicyStatement:
    """
        Return a Policy statement
    Args:
        actions: permissions list example = ['s3:PutObject']
        resources: "arn:s3//example"
        
    """
    return iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=actions,
        resources=resources,
        principals=principals
    )


def template_role(scope: Construct, id_name: str, role_name: str,service_name:str) -> iam.Role:
    """
        Return a Service Principal for use lambda
        Args:
            id_name: id of role
            role_name: name of the role in AWS
            service_name: service aws that will assume the Role
    """
    return iam.Role(
            scope=scope,
            id=id_name,
            assumed_by=iam.ServicePrincipal(f'{service_name}.amazonaws.com'),
            role_name=role_name
        )
def template_s3(scope:Construct,id_name:str ,bucket_name:str,versioned:bool,enforce_ssl:bool) -> s3.Bucket:
    return s3.Bucket(scope, id_name,
        bucket_name=bucket_name,
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        encryption=s3.BucketEncryption.S3_MANAGED,
        enforce_ssl=enforce_ssl,
        versioned=versioned,
        removal_policy=RemovalPolicy.DESTROY
    )


def template_event_bridge(
        scope: Construct,
        id_name: str,
        cron: events.Schedule,
        activated: bool,
        _lambda: _lambda.Function,
        json_object: dict
) -> events.Rule:

    event = events.Rule(
        scope=scope,
        id=id_name,
        schedule=cron,
        enabled=activated
    )

    event.add_target(
        targets.LambdaFunction(
            _lambda,
            event=events.RuleTargetInput.from_object(json_object)
        )
    )
    return event