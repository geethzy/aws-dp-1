# from constructs import Construct
# from aws_cdk import (
#     Duration,
#     Stack,
#     aws_s3 as s3,
#     aws_lambda as _lambda,
#     aws_lambda_event_sources as lambdaevent,
#     aws_glue as glue,
# )

# class FirstcdkStack(Stack):

#     def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
#         super().__init__(scope, construct_id, **kwargs)

#         rawbucket = s3.Bucket(
#             self, "inputBucket",
#         )

#         lambda_function = _lambda.Function(
#             self,
#             'cdkLambdaTest',
#             runtime=_lambda.Runtime.PYTHON_3_12,
#             handler='lambda_handler.handler',  
#             code=_lambda.Code.from_asset('lambda')  
#         )

#         rawbucket.grant_read(lambda_function)

#         lambda_function.add_event_source(
#             lambdaevent.S3EventSource(rawbucket, events=[s3.EventType.OBJECT_CREATED])
#         )

#         database = glue.CfnDatabase(
#             self, "MyGlueDatabase",
#             database_input=glue.CfnDatabase.DatabaseInputProperty(
#                 name="cdk_database"
#             )
#         )

#         crawler = glue.CfnCrawler(
#             self, "cdkCrawler",
#             role="glue_service_role_arn",
#             targets={"s3Targets": [{"path": "s3://ipbucket/csv/"}]}, 
#             database_name=database.attr_name,
#             name="test-lambda-crawler"
#         )

#         lambda_function.add_to_role_policy(
#             statement=glue.CfnCrawler.start_crawler.create_iam_policy(database.database_name)
#         )


import aws_cdk as cdk
import aws_cdk.aws_s3 as _s3
import aws_cdk.aws_iam as _iam
import aws_cdk.aws_sqs as _sqs
import aws_cdk.aws_glue as _glue
from constructs import Construct
import aws_cdk.aws_events as _events
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_lakeformation as _lakeformation

# SQS - 
# Use Case: This setup is useful when you need immediate or direct processing of the S3 file upload events.
# JSON Structure: The event data sent to the SQS queue typically contains information specific to the S3 event, such as the bucket name, object key, event type, etc.
# EventBridge Rule 
# Use Case: This setup is beneficial when you need to orchestrate a more complex workflow or require additional event filtering or transformation before triggering the workflow.
# JSON Structure: The event pattern defined in the EventBridge rule filters and shapes the event data before triggering the Glue Workflow. 
class FirstcdkStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        
        raw_bucket = _s3.Bucket(self, 'raw_data', #logical ID within CF template
                                bucket_name=f'raw-cdk-bucket-{cdk.Aws.ACCOUNT_ID}', #can make the name unique
                                # auto_delete_objects=True, #enable auto deletion of objects with CF stack deletion
                                removal_policy=cdk.RemovalPolicy.DESTROY, #enable auto deletion of buckets (with objects) with CF stack deletion
                                event_bridge_enabled=True,
                                lifecycle_rules=[_s3.LifecycleRule( #to do transition/expiration actions
                                    transitions=[_s3.Transition(
                                        storage_class=_s3.StorageClass.GLACIER, #objects to lowcost & longterm storage
                                        transition_after=cdk.Duration.days(7))])]) #after 7 days objects transit to the glacier storage

        # _s3.Bucket(self, 'processed_data',
        #            bucket_name=f'processed-cdk-bucket-{cdk.Aws.ACCOUNT_ID}',
        #            auto_delete_objects=True,
        #            removal_policy=cdk.RemovalPolicy.DESTROY)

        # scripts_bucket = _s3.Bucket(self, 'glue_scripts',
        #                             bucket_name=f'glue-scripts-{cdk.Aws.ACCOUNT_ID}',
        #                             auto_delete_objects=True,
        #                             removal_policy=cdk.RemovalPolicy.DESTROY)

        # # Upload glue script to the specified bucket
        # s3deploy.BucketDeployment(self, 'deployment',
        #                           sources=[s3deploy.Source.asset('./assets/')],
        #                           destination_bucket=scripts_bucket)

        output_bucket = _s3.Bucket(self, 'processed_cdk_bucket',
                   bucket_name=f'processed-cdk-bucket-{cdk.Aws.ACCOUNT_ID}',
                   auto_delete_objects=True,
                   removal_policy=cdk.RemovalPolicy.DESTROY)

        s3deploy.BucketDeployment(self, 'deployment_scripts',
                          sources=[s3deploy.Source.asset('./assets/')],
                          destination_bucket=output_bucket,
                          destination_key_prefix='scripts/')

        # The following combination allows you to crawl only files from the event 
        # instead of recrawling the whole S3 bucket, thus improving Glue Crawler’s 
        # performance and reducing its cost.
        glue_queue = _sqs.Queue(self, 'glue_queue')
        raw_bucket.add_event_notification(_s3.EventType.OBJECT_CREATED, s3n.SqsDestination(glue_queue))

        glue_role = _iam.Role(self, 'glue_role',
                              role_name='cdkGlueRole',
                              description='Role for Glue services to access S3',
                              assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'), # the trusted entityservice
                              inline_policies={'glue_policy': _iam.PolicyDocument(statements=[_iam.PolicyStatement(
                                  effect=_iam.Effect.ALLOW,
                                  actions=['s3:*', 'glue:*', 'iam:*', 'logs:*', 'cloudwatch:*', 'sqs:*',
                                           'cloudtrail:*'],
                                  resources=['*'])])})

        glue_database = _glue.CfnDatabase(self, 'glue-database',
                                          catalog_id=cdk.Aws.ACCOUNT_ID,
                                          database_input=_glue.CfnDatabase.DatabaseInputProperty(
                                              name='cdk-database',
                                              description='Database to store csv data.'))
        # grants full ('ALL') LakeFormation permissions to the glue_role IAM role
        # LakeFormation is an AWS service that helps manage and govern data lakes by 
        # centralizing access controls and permissions across various AWS services like 
        # AWS Glue, S3, and Athena.
        _lakeformation.CfnPermissions(self, 'lakeformation_permission',
                                      data_lake_principal=_lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                          data_lake_principal_identifier=glue_role.role_arn), # links permisson to the glue role
                                      resource=_lakeformation.CfnPermissions.ResourceProperty(
                                          database_resource=_lakeformation.CfnPermissions.DatabaseResourceProperty(
                                              catalog_id=glue_database.catalog_id,
                                              name='cdk-database')),
                                      permissions=['ALL'])

        _glue.CfnCrawler(self, 'glue_crawler',
                         name='cdk_crawler',
                         role=glue_role.role_arn,
                         database_name='cdk-database',
                         targets=_glue.CfnCrawler.TargetsProperty(
                             s3_targets=[_glue.CfnCrawler.S3TargetProperty(
                                 path=f's3://{raw_bucket.bucket_name}/csv/',
                                 event_queue_arn=glue_queue.queue_arn)]),
                         recrawl_policy=_glue.CfnCrawler.RecrawlPolicyProperty(
                             recrawl_behavior='CRAWL_EVENT_MODE')) #to crawl only changes identified by Amazon S3 events hence only new or updated files are in Glue Crawler’s scope, not entire S3 bucket.

        glue_job = _glue.CfnJob(self, 'glue_job',
                                name='glue_job',
                                command=_glue.CfnJob.JobCommandProperty(
                                    name='pythonshell', # script type
                                    python_version='3.12',
                                    script_location=f's3://{output_bucket.bucket_name}/glue_job.py'),
                                role=glue_role.role_arn,
                                glue_version='4.0',
                                timeout=3)

        #For a Spark history
#         _glue.CfnJob(self, 'glue_job',
#     name='cdk_glue_spark_job',
#     command=_glue.CfnJob.JobCommandProperty(
#         name='glueetl',
#         script_location=f's3://{output_bucket.bucket_name}/spark_script.py',
#         python_version='3',
#         script_location=f's3://{output_bucket.bucket_name}/spark_script.py',
#         job_bookmark_option='job-bookmark-enable'  # Enable job bookmarks for Spark job
#     ),
#     role=glue_role.role_arn,
#     glue_version='3.0',
#     timeout=3
# )

# AWS Glue workflows are used to orchestrate and schedule multiple Glue jobs, triggers, and crawlers in a sequenced manner. 
        glue_workflow = _glue.CfnWorkflow(self, 'glue_workflow',
                                          name='glue_workflow',
                                          description='Workflow to process the rfm data.')

        _glue.CfnTrigger(self, 'glue_crawler_trigger',
                         name='glue_crawler_trigger',
                         actions=[_glue.CfnTrigger.ActionProperty(
                             crawler_name='cdk_crawler',
                             notification_property=_glue.CfnTrigger.NotificationPropertyProperty(notify_delay_after=3),
                             timeout=3)],
                         type='EVENT',
                         workflow_name=glue_workflow.name)
        
        _glue.CfnTrigger(self, 'glue_job_trigger',
                         name='glue_job_trigger',
                         actions=[_glue.CfnTrigger.ActionProperty(
                             job_name=glue_job.name,
                             notification_property=_glue.CfnTrigger.NotificationPropertyProperty(notify_delay_after=3),
                             timeout=3)],
                         type='CONDITIONAL',
                         start_on_creation=True,
                         workflow_name=glue_workflow.name,
                         predicate=_glue.CfnTrigger.PredicateProperty(
                             conditions=[_glue.CfnTrigger.ConditionProperty(
                                 crawler_name='glue_crawler',
                                 logical_operator='EQUALS',
                                 crawl_state='SUCCEEDED')]))


        rule_role = _iam.Role(self, 'rule_role',
                              role_name='EventBridgeRole',
                              description='Role for EventBridge to trigger Glue workflows.',
                              assumed_by=_iam.ServicePrincipal('events.amazonaws.com'),
                              inline_policies={
                                  'eventbridge_policy': _iam.PolicyDocument(statements=[_iam.PolicyStatement(
                                      effect=_iam.Effect.ALLOW,
                                      actions=['events:*', 'glue:*'],
                                      resources=['*'])])})

        _events.CfnRule(self, 'rule_s3_glue',
                        name='rule_s3_glue',
                        role_arn=rule_role.role_arn,
                        targets=[_events.CfnRule.TargetProperty(
                            arn=f'arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workflow/glue_workflow',
                            role_arn=rule_role.role_arn,
                            id=cdk.Aws.ACCOUNT_ID)],
                        event_pattern={
                            "detail-type": ["Object Created"],
                            "detail": {
                                "bucket": {"name": [f"{raw_bucket.bucket_name}"]}},
                            "source": ["aws.s3"]})