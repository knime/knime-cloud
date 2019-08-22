# ![Image](https://www.knime.com/files/knime_logo_github_40x40_4layers.png) KNIME® Cloud Connectors

This repository contains:

* KNIME Amazon Cloud Connectors  
This extension enables the use of the KNIME filehandling nodes with Amazon S3.
* KNIME Amazon Machine Learning Integration  
This extension provides nodes for interaction with Amazon Comprehend and Amazon Translate.
* KNIME Amazon Redshift Connector And Tools  
This extension provides nodes for interacting with Amazon Redshift.
* KNIME Amazon Athena Connector  
This extension contains the Amazon Athena connector node.
* KNIME Azure Cloud Connectors  
This extension enables the use of the KNIME filehandling nodes with the Microsoft Azure Blob Storage.

## Overview

This repository contains the source code for the KNIME Cloud Connectors.
The extensions contain the following nodes:

* KNIME Amazon Cloud Connectors
    * **Amazon Authentication**: Configures the connection information used to connect to  several Amazon services. (For now this can only be used for with the KNIME Amazon Machine Learning Integration)
    * **Amazon S3 Connection**: Configures the connection information used to
        connect to Amazon S3.
    * **Amazon S3 File Picker**: Generates a pre-signed URL of an Amazon S3 object.
* KNIME Amazon Machine Learning Integration
    * **Amazon Comprehend (Key Phrases)**: Locates key phrases in text utilizing the Amazon Comprehend service.
    * **Amazon Comprehend (Dominant Language)**: Detects the dominant language of a text by using the Amazon Comprehend service.
    * **Amazon Comprehend (Sentiment)**: Performs sentiment analysis on sentences of a document by using the Amazon Comprehend service.
    * **Amazon Comprehend (Entity Tagger)**: Assigns named-entity tags to terms by calling the AWS Comprehend service.
    * **Amazon Comprehend (Syntax Tagger)**: Assigns part-of-speech tags to terms by calling the AWS Comprehend service.
    * **Amazon Translate**: Translates text from one language to another using the Amazon Translate service.
* KNIME Amazon Redshift Connector And Tools
    * **Amazon Redshift Cluster Launcher**: Creates an Amazon Redshift Cluster.
    * **Amazon Redshift Cluster Deleter**: Delete an Amazon Redshift Cluster according to the settings provided.
    * **Amazon Redshift Connector (legacy)**: Create a database connection to Amazon Redshift.
    * **Amazon Redshift Connector**: Create a database connection to Amazon Redshift.
* KNIME Azure Cloud Connectors
    * **Azure Blob Store Connection**: Configures the connection information used to
        connect to Azure Blob Store.
    * **Azure Blob Store File Picker**: Generates a pre-signed URL for a blob.

## Example Workflows on the KNIME Hub

You can find example workflows on the [KNIME Hub](https://hub.knime.com/search?q=s3,redshift,athena,amazon,azure&type=Workflow).

## Development Notes

You can find instructions on how to work with our code or develop extensions for
KNIME Analytics Platform in the _knime-sdk-setup_ repository
on [BitBucket](https://bitbucket.org/KNIME/knime-sdk-setup)
or [GitHub](http://github.com/knime/knime-sdk-setup).

## Join the Community!

* [KNIME Forum](https://tech.knime.org/forum)