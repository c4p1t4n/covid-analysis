from aws_cdk import (
    # Duration,
    Stack,
    aws_ecs as ecs,
    aws_logs as logs,
    aws_sqs as sqs,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_sns as sns,
    aws_s3_notifications as s3n,
    aws_sns_subscriptions as subs,
    aws_lambda_event_sources as lambda_event_sources,
    aws_athena as athena,
    aws_glue as glue
)
from constructs import Construct
from infrastructure.resources.resources import *
class CovidForecastStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        def create_ec2():
            vpc = ec2.Vpc(self, "MyVpc",
                    max_azs=1,
                    nat_gateways=0,
                    subnet_configuration=[ec2.SubnetConfiguration(
                        name="PublicSubnet",
                        subnet_type=ec2.SubnetType.PUBLIC,
                    )])


            security_group = ec2.SecurityGroup(
                        self
                        ,"SecurityGroup",
                        vpc=vpc,
                        description="Allow ssh and web access",
                        allow_all_outbound=True)
            security_group.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(22), "Allow SSH access")
            security_group.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(80), "Allow HTTP access")

        # Create an EC2 instance
            instance = ec2.Instance(self, "Instance",
                                instance_type=ec2.InstanceType("t2.micro"),
                                machine_image=ec2.MachineImage.generic_linux({"us-east-1":"ami-04b70fa74e45c3917"}),
                                vpc=vpc,
                                security_group=security_group,
                                key_name="your-key-pair-name")



        def stack_raw_to_stage_serverless():
            sns_topic_raw= template_sns_topic(self,"topic_sns_raw_to_stage","topic_sns_raw_to_stage_covid_forecast")
            bucket_raw_covid_forecast =  template_s3(self,"covid-forecast-raw-4288","covid-forecast-raw-4288",False,True)
            bucket_raw_covid_forecast.add_event_notification(s3.EventType.OBJECT_CREATED_PUT,s3n.SnsDestination(sns_topic_raw))
            sqs_raw_to_stage = template_sqs_queue(self,'sqs_raw_to_stage','sqs_raw_to_stage',visibility_timeout=120)
            lambda_raw = template_lambda(scope=self
                                            ,iam_role=role_lambda
                                            ,lambda_name='raw_to_stage'
                                            ,lambda_handler='raw_to_stage.lambda_handler'
                                            ,code_path = 'src/cloud_infrastructure/raw_to_stage/'
                                            ,id_name='lambda_raw_to_stage'
                                            ,minutes=2)
            

            sns_topic_raw.add_subscription(subs.SqsSubscription(sqs_raw_to_stage))
            lambda_raw.add_event_source(lambda_event_sources.SqsEventSource(sqs_raw_to_stage))

        def stack_stage_to_processed_serverless():
            sns_topic_stage = template_sns_topic(self,"topic_sns_stage_to_processed","topic_sns_stage_to_processed_covid_forecast")
            bucket_stage_covid_forecast =  template_s3(self,"covid-forecast-stage-4288","covid-forecast-stage-4288",False,True)
            bucket_stage_covid_forecast.add_event_notification(s3.EventType.OBJECT_CREATED_PUT,s3n.SnsDestination(sns_topic_stage))
            sqs_stage_to_processed = template_sqs_queue(self,'sqs_stage_to_processed','sqs_stage_to_processed',visibility_timeout=120)
            lambda_stage_none = template_lambda_none(self,role_lambda,'lambda_stage_to_processed','lambda_stage_to_processed',2)
            sns_topic_stage.add_subscription(subs.SqsSubscription(sqs_stage_to_processed))
            lambda_stage_none.add_event_source(lambda_event_sources.SqsEventSource(sqs_stage_to_processed,batch_size=1))
            bucket_stage_covid_forecast =  template_s3(self,"covid-forecast-processed-4288","covid-forecast-processed-4288",False,True)
        role_lambda = template_role(self,'role_lambda_covid_forecast','role_lambda_covid_forecast','lambda')


        s3_actions = template_iam(
            actions=[
                "*"
            ],
            resources=['*']    
        )
        logs_actions = template_iam(
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",


                
                "logs:DescribeLogGroups",
            ],
            resources=['*']    
        )

        policy_lambda = template_policy(
            scope=self,
            id_name = 'policy_lambda',
            statements=[
                s3_actions,
                logs_actions
            ]
        )

        role_lambda.attach_inline_policy(policy_lambda)
        stack_raw_to_stage_serverless()
        stack_stage_to_processed_serverless()

        glue_database = glue.CfnDatabase(self, "glue_database",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="covid_database_glue"
            )
        )

        glue_table = glue.CfnTable(self, "covid_report",
            catalog_id=self.account,
            database_name=glue_database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="covid_weekly_report",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "compressionType": "none",
                    "typeOfData": "file"
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="codigo_ibge", type="string"),
                        glue.CfnTable.ColumnProperty(name="semana_epidem", type="string"),
                        glue.CfnTable.ColumnProperty(name="casos", type="int"),
                        glue.CfnTable.ColumnProperty(name="casos_novos", type="int"),
                        glue.CfnTable.ColumnProperty(name="obitos", type="int"),
                        glue.CfnTable.ColumnProperty(name="obitos_novos", type="int"),
                        glue.CfnTable.ColumnProperty(name="casos_pc", type="double"),
                        glue.CfnTable.ColumnProperty(name="casos_mm4w", type="double"),
                        glue.CfnTable.ColumnProperty(name="obitos_pc", type="double"),
                        glue.CfnTable.ColumnProperty(name="obitos_mm4w", type="double"),
                        glue.CfnTable.ColumnProperty(name="letalidade", type="double"),
                        glue.CfnTable.ColumnProperty(name="nome_ra", type="string"),
                        glue.CfnTable.ColumnProperty(name="cod_ra", type="string"),
                        glue.CfnTable.ColumnProperty(name="pop", type="int"),
                        glue.CfnTable.ColumnProperty(name="pop60", type="int"),
                        glue.CfnTable.ColumnProperty(name="area", type="int"),
                        glue.CfnTable.ColumnProperty(name="map_leg", type="string"),
                        glue.CfnTable.ColumnProperty(name="map_leg_s", type="string"),
                        glue.CfnTable.ColumnProperty(name="latitude", type="double"),
                        glue.CfnTable.ColumnProperty(name="longitude", type="double"),
                        glue.CfnTable.ColumnProperty(name="datahora", type="datetime"),
                    ],
                    location=f"s3://covid-forecast-processed-4288/covid_daily/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    )
                ),
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="nome_drs", type="string"),
                    glue.CfnTable.ColumnProperty(name="nome_munic", type="string")
                ]
            )
        )
        query_results_bucket = s3.Bucket(self, "AthenaQueryResultsBucket")
        athena_workgroup = athena.CfnWorkGroup(self, "AthenaWorkGroup",
            name="my_workgroup",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=query_results_bucket.s3_url_for_object()
                )
            )
        )
