gmail_schema = r'''
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "additionalProperties": true,
  "properties": {
    "id": {
      "type": "string"
    },
    "thread_id": {
      "type": "string"
    },
    "snippet": {
      "type": "string"
    },
    "mail_date": {
      "type": "string"
    },
    "msg_from": {
      "type": "string"
    },
    "msg_to": {
      "type": "string"
    },
    "msg_subject": {
      "type": "string"
    },
    "body": {
      "type": "string"
    },   
    "historyId": {
      "type": "string"
    },
    "emailAddress": {
      "type": "string"
    },
    "customerSlug": {
      "type": "string"
    },
    "customerInstanceId": {
      "type": "string"
    },
    "customerId": {
      "type": "string"
    },
    "userId": {
      "type": "string"
    },
    "provider": {
      "type": "string"
    }
  }
}
'''