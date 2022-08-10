#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Tuple

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from requests.auth import HTTPBasicAuth
from source_greenhouse.streams import (
    Applications,
    ApplicationsDemographicsAnswers,
    ApplicationsInterviews,
    Candidates,
    CloseReasons,
    CustomFields,
    Degrees,
    DemographicsAnswerOptions,
    DemographicsAnswers,
    DemographicsAnswersAnswerOptions,
    DemographicsQuestions,
    DemographicsQuestionSets,
    DemographicsQuestionSetsQuestions,
    Departments,
    Interviews,
    JobPosts,
    Jobs,
    JobsOpenings,
    JobsStages,
    JobStages,
    Offers,
    RejectionReasons,
    Scorecards,
    Sources,
    Users,
)


class SourceGreenhouse(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            auth = HTTPBasicAuth(config["api_key"], "")
            users_gen = Users(authenticator=auth).read_records(sync_mode=SyncMode.full_refresh)
            next(users_gen)
            return True, None
        except Exception as error:
            return False, f"Unable to connect to Greenhouse API with the provided credentials - {repr(error)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = HTTPBasicAuth(config["api_key"], "")
        streams = [
            Applications(authenticator=auth, replication_start_date=config["replication_start_date"]),
            ApplicationsInterviews(authenticator=auth),
            Candidates(authenticator=auth, replication_start_date=config["replication_start_date"]),
            CloseReasons(authenticator=auth),
            CustomFields(authenticator=auth),
            Degrees(authenticator=auth),
            Departments(authenticator=auth),
            Interviews(authenticator=auth, replication_start_date=config["replication_start_date"]),
            JobPosts(authenticator=auth, replication_start_date=config["replication_start_date"]),
            JobStages(authenticator=auth, replication_start_date=config["replication_start_date"]),
            Jobs(authenticator=auth, replication_start_date=config["replication_start_date"]),
            JobsOpenings(authenticator=auth),
            JobsStages(authenticator=auth),
            Offers(authenticator=auth, replication_start_date=config["replication_start_date"]),
            RejectionReasons(authenticator=auth),
            Scorecards(authenticator=auth, replication_start_date=config["replication_start_date"]),
            Sources(authenticator=auth),
            Users(authenticator=auth, replication_start_date=config["replication_start_date"]),
            ApplicationsDemographicsAnswers(authenticator=auth),
            DemographicsAnswers(authenticator=auth),
            DemographicsAnswerOptions(authenticator=auth),
            DemographicsQuestions(authenticator=auth),
            DemographicsAnswersAnswerOptions(authenticator=auth),
            DemographicsQuestionSets(authenticator=auth),
            DemographicsQuestionSetsQuestions(authenticator=auth),
        ]

        return streams
