#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import base64
import hashlib


def hash(value, hash_type="md5", salt=None):
    """
      Implementation of a custom Jinja2 hash filter
      Hash type defaults to 'md5' if one is not specified.

      If you are using this has function for GDPR compliance, then
      you should probably also pass in a salt as discussed in:
      https://security.stackexchange.com/questions/202022/hashing-email-addresses-for-gdpr-compliance

      This can be used in a low code connector definition under the AddFields transformation.
      For example:

    rates_stream:
      $ref: "#/definitions/base_stream"
      $parameters:
        name: "rates"
        primary_key: "date"
        path: "/exchangerates_data/latest"
      transformations:
        - type: AddFields
          fields:
            - path: ["some_new_path"]
              value: "{{ record['rates']['CAD'] | hash('md5', 'mysalt')  }}"



      :param value: value to be hashed
      :return: computed hash as a hexadecimal string
    """
    hash_func = getattr(hashlib, hash_type, None)

    if hash_func:
        hash_obj = hash_func()
        hash_obj.update(str(value).encode("utf-8"))
        if salt:
            hash_obj.update(str(salt).encode("utf-8"))
        computed_hash = hash_obj.hexdigest()
    else:
        raise AttributeError("No hashing function named {hname}".format(hname=hash_type))

    return computed_hash


def base64encode(value: str) -> str:
    """
    Implementation of a custom Jinja2 base64encode filter

    For example:

      OAuthAuthenticator:
        $ref: "#/definitions/OAuthAuthenticator"
        $parameters:
          name: "client_id"
          value: "{{ config['client_id'] | base64encode }}"

    :param value: value to be encoded in base64
    :return: base64 encoded string
    """

    return base64.b64encode(value.encode("utf-8")).decode()


def base64decode(value: str) -> str:
    """
    Implementation of a custom Jinja2 base64decode filter

    For example:

      OAuthAuthenticator:
        $ref: "#/definitions/OAuthAuthenticator"
        $parameters:
          name: "client_id"
          value: "{{ config['client_id'] | base64encode }}"

    :param value: value to be decoded from base64
    :return: base64 decoded string
    """

    return base64.b64decode(value.encode("utf-8")).decode()


_filters_list = [hash, base64encode, base64decode]
filters = {f.__name__: f for f in _filters_list}
