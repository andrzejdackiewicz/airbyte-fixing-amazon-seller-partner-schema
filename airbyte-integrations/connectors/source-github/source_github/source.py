#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Dict, List, Mapping, Tuple

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import FailureType, SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import MultipleTokenAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from .streams import (
    Assignees,
    Branches,
    Collaborators,
    Comments,
    CommitCommentReactions,
    CommitComments,
    Commits,
    Deployments,
    Events,
    IssueCommentReactions,
    IssueEvents,
    IssueLabels,
    IssueMilestones,
    IssueReactions,
    Issues,
    Organizations,
    ProjectCards,
    ProjectColumns,
    Projects,
    PullRequestCommentReactions,
    PullRequestCommits,
    PullRequests,
    PullRequestStats,
    Releases,
    Repositories,
    RepositoryStats,
    ReviewComments,
    Reviews,
    Stargazers,
    Tags,
    TeamMembers,
    TeamMemberships,
    Teams,
    Users,
    WorkflowJobs,
    WorkflowRuns,
    Workflows,
)
from .utils import read_full_refresh

TOKEN_SEPARATOR = ","
DEFAULT_PAGE_SIZE_FOR_LARGE_STREAM = 10


class SourceGithub(AbstractSource):

    @staticmethod
    def _get_org_repositories(config: Mapping[str, Any], authenticator: MultipleTokenAuthenticator) -> Tuple[List[str], List[str]]:
        """
        Parse config.repository and produce two lists: organizations, repositories.
        Args:
            config (dict): Dict representing connector's config
            authenticator(MultipleTokenAuthenticator): authenticator object
        """
        config_repositories = set(filter(None, config["repository"].split(" ")))
        if not config_repositories:
            raise Exception("Field `repository` required to be provided for connect to Github API")

        repositories = set()
        organizations = set()
        unchecked_repos = set()
        unchecked_orgs = set()

        for org_repos in config_repositories:
            org, _, repos = org_repos.partition("/")
            if repos == "*":
                unchecked_orgs.add(org)
            else:
                unchecked_repos.add(org_repos)

        if unchecked_orgs:
            stream = Repositories(authenticator=authenticator, organizations=unchecked_orgs)
            for record in read_full_refresh(stream):
                repositories.add(record["full_name"])
                organizations.add(record["organization"])

        unchecked_repos = unchecked_repos - repositories
        if unchecked_repos:
            stream = RepositoryStats(
                authenticator=authenticator,
                repositories=unchecked_repos,
                page_size_for_large_streams=config.get("page_size_for_large_streams", DEFAULT_PAGE_SIZE_FOR_LARGE_STREAM),
                config=config
            )
            for record in read_full_refresh(stream):
                repositories.add(record["full_name"])
                organization = record.get("organization", {}).get("login")
                if organization:
                    organizations.add(organization)

        return list(organizations), list(repositories)

    @staticmethod
    def _get_authenticator(config: Dict[str, Any]):
        # Before we supported oauth, personal_access_token was called `access_token` and it lived at the
        # config root. So we first check to make sure any backwards compatbility is handled.
        token = config.get("access_token")
        if not token:
            creds = config.get("credentials")
            token = creds.get("access_token") or creds.get("personal_access_token")
        tokens = [t.strip() for t in token.split(TOKEN_SEPARATOR)]
        return MultipleTokenAuthenticator(tokens=tokens, auth_method="token")

    @staticmethod
    def _get_branches_data(selected_branches: str, config: Mapping[str, Any], full_refresh_args: Dict[str, Any] = None) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
        selected_branches = set(filter(None, selected_branches.split(" ")))

        # Get the default branch for each repository
        default_branches = {}
        repository_stats_stream = RepositoryStats(config, **full_refresh_args)
        for stream_slice in repository_stats_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            default_branches.update(
                {
                    repo_stats["full_name"]: repo_stats["default_branch"]
                    for repo_stats in repository_stats_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice)
                }
            )

        all_branches = []
        branches_stream = Branches(config, **full_refresh_args)
        for stream_slice in branches_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            for branch in branches_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice):
                all_branches.append(f"{branch['repository']}/{branch['name']}")

        # Create mapping of repository to list of branches to pull commits for
        # If no branches are specified for a repo, use its default branch
        branches_to_pull: Dict[str, List[str]] = {}
        for repo in full_refresh_args["repositories"]:
            repo_branches = []
            for branch in selected_branches:
                branch_parts = branch.split("/", 2)
                if "/".join(branch_parts[:2]) == repo and branch in all_branches:
                    repo_branches.append(branch_parts[-1])
            if not repo_branches:
                repo_branches = [default_branches[repo]]

            branches_to_pull[repo] = repo_branches

        return default_branches, branches_to_pull

    def user_friendly_error_message(self, message: str) -> str:
        user_message = ""
        if "404 Client Error: Not Found for url: https://api.github.com/repos/" in message:
            # 404 Client Error: Not Found for url: https://api.github.com/repos/airbytehq/airbyte3?per_page=100
            full_repo_name = message.split("https://api.github.com/repos/")[1].split("?")[0]
            user_message = f'Repo name: "{full_repo_name}" is unknown, "repository" config option should use existing full repo name <organization>/<repository>'
        elif "404 Client Error: Not Found for url: https://api.github.com/orgs/" in message:
            # 404 Client Error: Not Found for url: https://api.github.com/orgs/airbytehqBLA/repos?per_page=100
            org_name = message.split("https://api.github.com/orgs/")[1].split("/")[0]
            user_message = f'Organization name: "{org_name}" is unknown, "repository" config option should be updated'
        elif "401 Client Error: Unauthorized for url" in message:
            # 401 Client Error: Unauthorized for url: https://api.github.com/orgs/datarootsio/repos?per_page=100&sort=updated&direction=desc
            user_message = "Bad credentials, re-authentication or access token renewal is required"
        return user_message

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            authenticator = self._get_authenticator(config)
            _, repositories = self._get_org_repositories(config=config, authenticator=authenticator)
            if not repositories:
                return False, "no valid repositories found"
            return True, None

        except Exception as e:
            message = repr(e)
            user_message = self.user_friendly_error_message(message)
            return False, user_message or message

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self._get_authenticator(config)
        try:
            organizations, repositories = self._get_org_repositories(config=config, authenticator=authenticator)
        except Exception as e:
            message = repr(e)
            user_message = self.user_friendly_error_message(message)
            if user_message:
                raise AirbyteTracedException(
                    internal_message=message, message=user_message, failure_type=FailureType.config_error, exception=e
                )
            else:
                raise e

        if not any((organizations, repositories)):
            raise Exception("No streams available. Please check permissions")

        page_size = config.get("page_size_for_large_streams", DEFAULT_PAGE_SIZE_FOR_LARGE_STREAM)

        organization_args = {"authenticator": authenticator, "organizations": organizations}
        organization_args_with_config = {"authenticator": authenticator, "organizations": organizations, "config": config}
        organization_args_with_start_date_and_config = {**organization_args, "start_date": config["start_date"], "config": config}
        repository_args = {"authenticator": authenticator, "repositories": repositories, "page_size_for_large_streams": page_size}
        repository_args_with_config = {"authenticator": authenticator, "repositories": repositories, "page_size_for_large_streams": page_size, "config": config}
        repository_args_with_start_date_and_config = {**repository_args, "start_date": config["start_date"], "config": config}

        default_branches, branches_to_pull = self._get_branches_data(config.get("branch", ""), config, repository_args)
        pull_requests_stream = PullRequests(**repository_args_with_start_date_and_config)
        projects_stream = Projects(**repository_args_with_start_date_and_config)
        project_columns_stream = ProjectColumns(projects_stream, **repository_args_with_start_date_and_config)
        teams_stream = Teams(**organization_args_with_config)
        team_members_stream = TeamMembers(parent=teams_stream, **repository_args_with_config)
        workflow_runs_stream = WorkflowRuns(**repository_args_with_start_date_and_config)

        return [
            Assignees(**repository_args_with_config),
            Branches(**repository_args_with_config),
            Collaborators(**repository_args_with_config),
            Comments(**repository_args_with_start_date_and_config),
            CommitCommentReactions(**repository_args_with_start_date_and_config),
            CommitComments(**repository_args_with_start_date_and_config),
            Commits(**repository_args_with_start_date_and_config, branches_to_pull=branches_to_pull, default_branches=default_branches),
            Deployments(**repository_args_with_start_date_and_config),
            Events(**repository_args_with_start_date_and_config),
            IssueCommentReactions(**repository_args_with_start_date_and_config),
            IssueEvents(**repository_args_with_start_date_and_config),
            IssueLabels(**repository_args_with_config),
            IssueMilestones(**repository_args_with_start_date_and_config),
            IssueReactions(**repository_args_with_start_date_and_config),
            Issues(**repository_args_with_start_date_and_config),
            Organizations(**organization_args_with_config),
            ProjectCards(project_columns_stream, **repository_args_with_start_date_and_config),
            project_columns_stream,
            projects_stream,
            PullRequestCommentReactions(**repository_args_with_start_date_and_config),
            PullRequestCommits(parent=pull_requests_stream, **repository_args_with_config),
            PullRequestStats(**repository_args_with_start_date_and_config),
            pull_requests_stream,
            Releases(**repository_args_with_start_date_and_config),
            Repositories(**organization_args_with_start_date_and_config),
            ReviewComments(**repository_args_with_start_date_and_config),
            Reviews(**repository_args_with_start_date_and_config),
            Stargazers(**repository_args_with_start_date_and_config),
            Tags(**repository_args_with_config),
            teams_stream,
            team_members_stream,
            Users(**organization_args_with_config),
            Workflows(**repository_args_with_start_date_and_config),
            workflow_runs_stream,
            WorkflowJobs(parent=workflow_runs_stream, **repository_args_with_start_date_and_config),
            TeamMemberships(parent=team_members_stream, **repository_args_with_config),
        ]
    