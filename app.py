#!/usr/bin/env python3

import aws_cdk as cdk

from firstcdk.firstcdk_stack import FirstcdkStack

app = cdk.App()
FirstcdkStack(app, "FirstcdkStack")

app.synth()
