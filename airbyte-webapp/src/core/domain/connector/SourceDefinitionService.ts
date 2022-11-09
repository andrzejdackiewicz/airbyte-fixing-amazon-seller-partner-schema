import { AirbyteRequestService } from "core/request/AirbyteRequestService";

import {
  createCustomSourceDefinition,
  CustomSourceDefinitionCreate,
  getSourceDefinition,
  listLatestSourceDefinitions,
  listSourceDefinitionsForWorkspace,
  SourceDefinitionUpdate,
  updateSourceDefinition,
} from "../../request/AirbyteClient";

export class SourceDefinitionService extends AirbyteRequestService {
  public get(sourceDefinitionId: string) {
    return getSourceDefinition({ sourceDefinitionId }, this.requestOptions);
  }

  public list(workspaceId: string) {
    return listSourceDefinitionsForWorkspace({ workspaceId }, this.requestOptions);
  }

  public listLatest() {
    return listLatestSourceDefinitions(this.requestOptions);
  }

  public update(body: SourceDefinitionUpdate) {
    return updateSourceDefinition(body, this.requestOptions);
  }

  public create(body: CustomSourceDefinitionCreate) {
    return createCustomSourceDefinition(body, this.requestOptions);
  }
}
