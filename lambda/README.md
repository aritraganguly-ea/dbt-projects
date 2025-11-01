# AWS Lambda — Run a Python ETL Script

This documentation explains how to package, configure, and deploy a Python ETL script to **AWS Lambda**. It presents three deployment approaches (zip, Lambda Layer, and container image) and includes example `handler.py`, packaging commands, IAM policies, a sample AWS SAM template, scheduling with EventBridge (CloudWatch Events), logging, monitoring, and best practices.

## Overview

This Lambda function runs two independent ETL pipelines defined in a single script, `lambda_handler.py`:

1. **CPIU ETL —** Extracts Consumer Price Index for All Urban Consumers (CPI-U) data from the U.S. Bureau of Labor Statistics (BLS), transforms it into a normalized monthly time series, and enriches it with metadata before storing the output as a CSV in S3.

2. **Transtrend ETL —** Extracts index return data from Transtrend’s API, normalizes and transforms it into a monthly return dataset, and saves the results to S3 with relevant metadata.

At runtime, the Lambda handler randomly selects one of the two pipelines (**CPIU** or **Transtrend**) to execute. This ensures that only a single ETL job runs per invocation.

The function is scheduled to run **every 3 hours** using **Amazon EventBridge**, which automatically triggers the Lambda function. Each execution logs details to CloudWatch and outputs a status message identifying which ETL pipeline was executed.

## Architecture

The architecture for this solution is as follows:

```bash
            EventBridge (every 3h)
                   ↓
            Lambda Function
                   |
                   ↓ (random choice)
┌───────────────┐    ┌────────────────┐
│    CPIU ETL   │ OR │ Transtrend ETL │
└───────────────┘    └────────────────┘
                   ↓
            Transform (pandas)
                   ↓
            Load CSV → S3 Bucket

Logs → CloudWatch
Metrics → CloudWatch
Secrets → Secrets Manager / Parameter Store (if required)
```

## Prerequisites

1. AWS account with permission to create IAM roles, Lambda functions, EventBridge rules, and S3 buckets.
2. AWS CLI configured (`aws configure`) or AWS SAM CLI installed.
3. Python version must match the Lambda runtime.
4. Docker (optional, needed for container deployments or `sam local`).

## How to create a Lambda Function?

Below are step-by-step instructions to create the Lambda function using three different methods: the **AWS Management Console (GUI)**, the **AWS CLI**, and **AWS SAM**. Use the method that best fits the desired workflow.

### Create via AWS Management Console (GUI).

1. Open the **AWS Management Console** and go to **Lambda**.
2. Click the **Create function**.
3. Choose **Author from scratch**.
   - **Function name:** `cpiu-transtrend-etl` (or any preferred name)
   - **Runtime:** Python 3.12 (or chosen runtime based on our requirement)
   - **Permissions:** choose **Create a new role from AWS policy templates** or **Use existing role**. If creating a new role, we can start with the **Lambda basic execution role policy** and add **S3 access** later.
4. Click the **Create function**.
5. In the **Configuration** tab:
   - **General configuration:** Set timeout (e.g., 600s) and memory (e.g., 256 MB).
   - **Permissions:** Attach or edit the execution role to include S3 and Secrets Manager permissions as needed.
   - **Environment variables:** Add `S3_BUCKET_NAME=cpui-transtrend-s3` and any other config as environment variables.
6. In the **Code** tab:

   - **Runtime settings:** Set Handler to `lambda_handler.lambda_handler` (module.function).
   - From the **Code** tab in the Lambda console, select **Add a layer**. AWS provides several prebuilt layers — for example, we can choose `AWSSDKPandas-Python312`, which includes `pandas` and other scientific libraries optimized for Lambda.

   If the project depends on additional or custom Python packages, we can create our own layer:

   - [Packaging layer content](https://docs.aws.amazon.com/lambda/latest/dg/packaging-layers.html) Package the dependencies into a .zip file (e.g., `python.zip` with the folder structure `python/lib/python3.x/site-packages/...`)
   - Install a compatible binary and zip it as a layer package.

   ```bash
       # Install a compatible binary.
       pip install \
       --platform manylinux2014_x86_64 \
       --implementation cp \
       --python-version 3.12 \
       --only-binary=:all: \
       --no-deps \
       --target=python/lib/python3.12/site-packages \
       lxml requests

       # Zip it as a layer package.
       zip -r ../lxml12-layer.zip python
   ```

   Using layers keeps the Lambda package lightweight, allows us to reuse dependencies across multiple functions, and makes it easier to update libraries without requiring a redeployment of the entire function.

   **Tip:** We can attach up to **5 layers per Lambda function**. Combine dependencies thoughtfully to avoid hitting the size limit (250 MB unzipped).

   - In the Lambda console, navigate to **Additional resources** → **Layers** → **Create layer**.
   - Upload the .zip file either directly or from an S3 bucket, provide a list of supported runtimes (such as Python 3.12), and then publish the layer.
   - Return to the Lambda function, click 'Add a layer,' and attach the custom layer.
   - Deploy the code each time any changes are made.
   - Save and test by creating a new test event using the Test button.

## Scheduling with EventBridge (AWS Console / GUI)

1. **Open the EventBridge Console:**

   - In the AWS Management Console, search for **EventBridge** and open it.

2. **Create a Rule:**

   - Click **EventBridge Rule** in the right navigation pane.
   - Select **Create rule**.

3. **Define Rule Details:**

   - Enter a **Name** (e.g., `cpiu-transtrend-scheduler`).
   - (Optional) Add a **Description**.
   - Leave the **Event bus** as `default` (unless you use a custom one).
   - Choose **Schedule** as the rule type.
   - Click **Continue in EventBridge Scheduler**.

4. **Configure the Schedule Pattern:**

   - Choose the **Recurring schedule**.
   - In the **Schedule type**, select **Rate-based schedule** and enter `2 hours`.
   - Alternatively, use a **Cron-based schedule** for more precise scheduling (e.g., `cron(0 */2 * * ? *)` for every 2 hours).
   - Enter the **Timeframe**, i.e., the optional `Start date and time` & `End date and time`.

5. **Select the Target:**

   - Under **Select targets**, choose **AWS service**.
   - From the drop-down, select **Lambda function**.
   - In **Function**, choose the deployed Lambda (e.g., `cpiu-transtrend-etl`).

6. **Configure Permissions:** If this is the first time linking EventBridge to Lambda, AWS will prompt us to create an **IAM permission** that allows EventBridge to invoke the Lambda. Accept this option (it will automatically create a resource policy).

7. **Create the Rule:**

   - Review all settings and click **Create rule**.

8. **Verify Setup:**

   - Go to the **Lambda function** → **Monitor tab** → **Recent invocations**.
   - Wait for the next 2-hour window or trigger the rule manually by clicking **Test** in EventBridge.

9. **Best Practices:**

   - Use **CloudWatch Logs** to confirm the Lambda is triggered on schedule.
   - Tag the EventBridge rule for easier cost allocation and auditing.
   - If multiple schedules are required (e.g., different ETLs at different frequencies), create separate rules pointing to the same Lambda and pass a custom **event payload** (JSON) to control which pipeline to run.
