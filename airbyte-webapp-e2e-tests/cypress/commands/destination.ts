import { deleteEntity, openSettingForm, submitButtonClick, updateField } from "./common";
import { fillLocalJsonForm } from "./connector"
import { goToDestinationPage, openNewDestinationForm } from "pages/destinationPage"

export const createLocalJsonDestination = (name: string, destinationPath: string) => {
  cy.intercept("/api/v1/scheduler/destinations/check_connection").as("checkDestinationConnection");
  cy.intercept("/api/v1/destinations/create").as("createDestination");

  goToDestinationPage();
  openNewDestinationForm();
  fillLocalJsonForm(name, destinationPath);
  submitButtonClick();

  cy.wait(3000);
  cy.wait("@checkDestinationConnection");
  cy.wait("@createDestination");
}

export const updateDestination = (name: string, field: string, value: string) => {
  cy.intercept("/api/v1/destinations/check_connection_for_update").as("checkDestinationUpdateConnection");
  cy.intercept("/api/v1/destinations/update").as("updateDestination");

  goToDestinationPage();
  openSettingForm(name);
  updateField(field, value);
  submitButtonClick();

  cy.wait("@checkDestinationUpdateConnection");
  cy.wait("@updateDestination");
}

export const deleteDestination = (name: string) => {
  cy.intercept("/api/v1/destinations/delete").as("deleteDestination");
  goToDestinationPage();
  openSettingForm(name);
  deleteEntity();
  cy.wait("@deleteDestination");
}