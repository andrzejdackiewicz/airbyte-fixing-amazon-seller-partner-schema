import { submitButtonClick } from "./common";
import { createLocalJsonDestination } from "./destination";
import { createPokeApiSource, createPostgresSource } from "./source";
import { openAddSource } from "pages/destinationPage"
import { selectSchedule, setupDestinationNamespaceSourceFormat, enterConnectionName } from "pages/replicationPage"

export const createTestConnection = (sourceName: string, destinationName: string) => {
  cy.intercept("/api/v1/sources/discover_schema").as("discoverSchema");
  cy.intercept("/api/v1/web_backend/connections/create").as("createConnection");

  switch (true) {
    case sourceName.includes('PokeAPI'):
      createPokeApiSource(sourceName, "luxray")
      break;
    case sourceName.includes('Postgres'):
      createPostgresSource(sourceName);
      break;
    default:
      createPostgresSource(sourceName);
  }

  createLocalJsonDestination(destinationName, "/local");
  cy.wait(5000);

  openAddSource();
  cy.get("div").contains(sourceName).click();

  cy.wait("@discoverSchema");

  enterConnectionName("Connection name");
  selectSchedule("Manual");

  setupDestinationNamespaceSourceFormat();
  submitButtonClick();

  cy.wait("@createConnection");
};
