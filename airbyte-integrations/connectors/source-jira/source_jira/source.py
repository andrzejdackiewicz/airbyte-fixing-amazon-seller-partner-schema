#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Optional, Tuple

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import BasicHttpAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from pydantic.error_wrappers import ValidationError

from .streams import (
    ApplicationRoles,
    Avatars,
    BoardIssues,
    Boards,
    Dashboards,
    Filters,
    FilterSharing,
    Groups,
    IssueComments,
    IssueCustomFieldContexts,
    IssueFieldConfigurations,
    IssueFields,
    IssueLinkTypes,
    IssueNavigatorSettings,
    IssueNotificationSchemes,
    IssuePriorities,
    IssueProperties,
    IssueRemoteLinks,
    IssueResolutions,
    Issues,
    IssueSecuritySchemes,
    IssueTransitions,
    IssueTypeSchemes,
    IssueTypeScreenSchemes,
    IssueVotes,
    IssueWatchers,
    IssueWorklogs,
    JiraSettings,
    Labels,
    Permissions,
    PermissionSchemes,
    ProjectAvatars,
    ProjectCategories,
    ProjectComponents,
    ProjectEmail,
    ProjectPermissionSchemes,
    Projects,
    ProjectTypes,
    ProjectVersions,
    PullRequests,
    Screens,
    ScreenSchemes,
    ScreenTabFields,
    ScreenTabs,
    SprintIssues,
    Sprints,
    TimeTracking,
    Users,
    UsersGroupsDetailed,
    Workflows,
    WorkflowSchemes,
    WorkflowStatusCategories,
    WorkflowStatuses,
)
from .utils import read_full_refresh


class SourceJira(AbstractSource):
    def _validate_and_transform(self, config: Mapping[str, Any]):
        config["projects"] = config.get("projects", [])
        return config

    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        return BasicHttpAuthenticator(config["email"], config["api_token"])

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        try:
            config = self._validate_and_transform(config)
            authenticator = self.get_authenticator(config)
            kwargs = {"authenticator": authenticator, "domain": config["domain"], "projects": config["projects"]}
            labels_stream = Labels(**kwargs)
            next(read_full_refresh(labels_stream), None)
            # check projects
            projects_stream = Projects(**kwargs)
            projects = {project["key"] for project in read_full_refresh(projects_stream)}
            unknown_projects = set(config["projects"]) - projects
            if unknown_projects:
                return False, "unknown project(s): " + ", ".join(unknown_projects)
            return True, None
        except ValidationError as validation_error:
            return False, validation_error
        except requests.exceptions.RequestException as request_error:
            if request_error.response.status_code == requests.codes.not_found:
                raise AirbyteTracedException(
                    message="Config validation error: please validate your domain.",
                    internal_message=str(request_error),
                    failure_type=FailureType.config_error,
                ) from None
            # sometimes jira returns non json response
            message = ""
            if request_error.response.headers.get("content-type") == "application/json":
                message = " ".join(map(str, request_error.response.json().get("errorMessages", "")))
            return False, f"{message} {request_error}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        config = self._validate_and_transform(config)
        authenticator = self.get_authenticator(config)
        args = {"authenticator": authenticator, "domain": config["domain"], "projects": config["projects"]}
        issues_stream = Issues(**args, expand_fields=config.get("issues_stream_expand_with", []))
        issue_fields_stream = IssueFields(**args)
        experimental_streams = []
        if config.get("enable_experimental_streams", False):
            experimental_streams.append(PullRequests(issues_stream=issues_stream, issue_fields_stream=issue_fields_stream, **args))
        return [
            ApplicationRoles(**args),
            Avatars(**args),
            Boards(**args),
            BoardIssues(**args),
            Dashboards(**args),
            Filters(**args),
            FilterSharing(**args),
            Groups(**args),
            issues_stream,
            IssueComments(**args),
            issue_fields_stream,
            IssueFieldConfigurations(**args),
            IssueCustomFieldContexts(**args),
            IssueLinkTypes(**args),
            IssueNavigatorSettings(**args),
            IssueNotificationSchemes(**args),
            IssuePriorities(**args),
            IssueProperties(**args),
            IssueRemoteLinks(**args),
            IssueResolutions(**args),
            IssueSecuritySchemes(**args),
            IssueTransitions(**args),
            IssueTypeSchemes(**args),
            IssueTypeScreenSchemes(**args),
            IssueVotes(**args),
            IssueWatchers(**args),
            IssueWorklogs(**args),
            JiraSettings(**args),
            Labels(**args),
            Permissions(**args),
            PermissionSchemes(**args),
            Projects(**args),
            ProjectAvatars(**args),
            ProjectCategories(**args),
            ProjectComponents(**args),
            ProjectEmail(**args),
            ProjectPermissionSchemes(**args),
            ProjectTypes(**args),
            ProjectVersions(**args),
            Screens(**args),
            ScreenTabs(**args),
            ScreenTabFields(**args),
            ScreenSchemes(**args),
            Sprints(**args),
            SprintIssues(**args),
            TimeTracking(**args),
            Users(**args),
            UsersGroupsDetailed(**args),
            Workflows(**args),
            WorkflowSchemes(**args),
            WorkflowStatuses(**args),
            WorkflowStatusCategories(**args),
        ] + experimental_streams
