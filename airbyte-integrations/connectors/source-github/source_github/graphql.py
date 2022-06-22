#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sgqlc.operation

from . import github_schema

_schema = github_schema
_schema_root = _schema.github_schema


def get_query_pull_requests(owner, name, first, after):
    kwargs = {"first": first, "order_by": {"field": "UPDATED_AT", "direction": "ASC"}}
    if after:
        kwargs["after"] = after

    op = sgqlc.operation.Operation(_schema_root.query_type)
    pull_requests = op.repository(owner=owner, name=name).pull_requests(**kwargs)
    pull_requests.nodes.__fields__(
        id="node_id",
        database_id="id",
        number=True,
        updated_at="updated_at",
        changed_files="changed_files",
        deletions=True,
        additions=True,
        merged=True,
        mergeable=True,
        can_be_rebased="can_be_rebased",
        maintainer_can_modify="maintainer_can_modify",
        merge_state_status="merge_state_status",
    )
    pull_requests.nodes.repository.__fields__(name=True)
    pull_requests.nodes.comments.__fields__(total_count=True)
    pull_requests.nodes.commits.__fields__(total_count=True)
    reviews = pull_requests.nodes.reviews(first=100, __alias__="review_comments")
    reviews.total_count()
    reviews.nodes.comments.__fields__(total_count=True)
    user = pull_requests.nodes.merged_by(__alias__="merged_by").__as__(_schema_root.User)
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    pull_requests.page_info.__fields__(has_next_page=True, end_cursor=True)
    return str(op)


def get_query_reviews(owner, name, first, after):
    kwargs = {"first": first, "order_by": {"field": "UPDATED_AT", "direction": "ASC"}}
    if after:
        kwargs["after"] = after

    op = sgqlc.operation.Operation(_schema_root.query_type)
    repository = op.repository(owner=owner, name=name)
    repository.name()
    repository.owner.login()
    pull_requests = repository.pull_requests(**kwargs)
    pull_requests.nodes.__fields__(url=True)
    reviews = pull_requests.nodes.reviews(first=first)
    reviews.nodes.__fields__(
        id="node_id",
        database_id="id",
        body=True,
        state=True,
        url="html_url",
        author_association="author_association",
        submitted_at="submitted_at",
        created_at="created_at",
        updated_at="updated_at",
    )
    reviews.nodes.commit.oid()
    user = reviews.nodes.author(__alias__="user").__as__(_schema_root.User)
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    pull_requests.page_info.__fields__(has_next_page=True, end_cursor=True)
    return str(op)
