import logging

import boto3
from botocore.exceptions import ClientError


def amazon_ses(parent_class):
    class AmazonEmail(parent_class):
        def _construct_and_send(self, subject):
            client = boto3.client('ses', region_name=self.region_name, aws_access_key_id=self.aws_access_key_id,
                                  aws_secret_access_key=self.aws_secret_access_key)
            try:
                client.send_email(
                    Destination={
                        'ToAddresses': self.recipients,
                        'CcAddresses': self.cc_addresses,
                        'BccAddresses': self.bcc_addresses
                    },
                    Message={
                        'Body': {
                            'Html': {
                                'Charset': 'UTF-8',
                                'Data': self.message.get_payload(),
                            }
                        },
                        'Subject': {
                            'Charset': 'UTF-8',
                            'Data': subject,
                        },
                    },
                    Source=self.original_from_address,
                )
            # Display an error if something goes wrong.
            except ClientError as e:
                logging.error(e.response['Error']['Message'])

    return AmazonEmail
