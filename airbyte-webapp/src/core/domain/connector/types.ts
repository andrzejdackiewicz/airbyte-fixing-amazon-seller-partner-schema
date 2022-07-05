import { DestinationDefinitionReadWithLatestTag } from "services/connector/DestinationDefinitionService";
import { SourceDefinitionReadWithLatestTag } from "services/connector/SourceDefinitionService";

import {
  DestinationDefinitionSpecificationRead,
  DestinationRead,
  SourceDefinitionSpecificationRead,
  SourceRead,
} from "../../request/AirbyteClient";

export type ConnectorDefinition = SourceDefinitionReadWithLatestTag | DestinationDefinitionReadWithLatestTag;

export type ConnectorDefinitionSpecification =
  | DestinationDefinitionSpecificationRead
  | SourceDefinitionSpecificationRead;

export type ConnectorT = DestinationRead | SourceRead;
