import React, { useCallback } from "react";
import { FormattedMessage } from "react-intl";
import { useOutletContext } from "react-router-dom";

import { DestinationRead } from "core/request/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";
import { useConnectionList } from "hooks/services/useConnectionHook";
import { useDeleteDestination, useUpdateDestination } from "hooks/services/useDestinationHook";
import { useDeleteModal } from "hooks/useDeleteModal";
import { useDestinationDefinition } from "services/connector/DestinationDefinitionService";
import { useGetDestinationDefinitionSpecification } from "services/connector/DestinationDefinitionSpecificationService";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorCardValues } from "views/Connector/ConnectorForm/types";

import styles from "./DestinationSettings.module.scss";

export const DestinationSettingsPage: React.FC = () => {
  const { destination } = useOutletContext<{ destination: DestinationRead }>();
  const { connections: connectionsWithDestination } = useConnectionList({ destinationId: [destination.destinationId] });
  const destinationSpecification = useGetDestinationDefinitionSpecification(destination.destinationDefinitionId);
  const destinationDefinition = useDestinationDefinition(destination.destinationDefinitionId);
  const { mutateAsync: updateDestination } = useUpdateDestination();
  const { mutateAsync: deleteDestination } = useDeleteDestination();
  const formId = useUniqueFormId();
  const { clearFormChange } = useFormChangeTrackerService();

  useTrackPage(PageTrackingCodes.DESTINATION_ITEM_SETTINGS);

  const onSubmitForm = async (values: ConnectorCardValues) => {
    await updateDestination({
      values,
      destinationId: destination.destinationId,
    });
  };

  const onDelete = useCallback(async () => {
    clearFormChange(formId);
    await deleteDestination({
      connectionsWithDestination,
      destination,
    });
  }, [clearFormChange, connectionsWithDestination, deleteDestination, destination, formId]);

  const onDeleteClick = useDeleteModal("destination", onDelete);

  return (
    <div className={styles.content}>
      <ConnectorCard
        formType="destination"
        title={<FormattedMessage id="destination.destinationSettings" />}
        isEditMode
        formId={formId}
        availableConnectorDefinitions={[destinationDefinition]}
        selectedConnectorDefinitionSpecification={destinationSpecification}
        selectedConnectorDefinitionId={destinationSpecification.destinationDefinitionId}
        connector={destination}
        onSubmit={onSubmitForm}
        onDeleteClick={onDeleteClick}
      />
    </div>
  );
};
