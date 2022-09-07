import { useField } from "formik";
import React from "react";
import { useIntl } from "react-intl";

import { ContentCard, ConnectorCard } from "components";

import { DestinationConnectorCard } from "../../types";
import styles from "./StartWithDestination.module.scss";

export interface StartWithDestinationProps {
  destination: DestinationConnectorCard | undefined;
  onDestinationSelect: ((id: string) => void) | undefined;
}

export const StartWithDestination: React.FC<StartWithDestinationProps> = ({ destination, onDestinationSelect }) => {
  // since we will use the component just in one place we can hardcode the useField()
  const [, , { setValue }] = useField("serviceType");
  const { formatMessage } = useIntl();

  if (!destination) {
    return null;
  }
  const { icon, releaseStage, name, destinationDefinitionId } = destination;

  const connectorCardClickHandler = () => {
    setValue(destinationDefinitionId);
    if (onDestinationSelect) {
      onDestinationSelect(destinationDefinitionId);
    }
  };

  return (
    <div className={styles.wrapper}>
      <button className={styles.container} onClick={connectorCardClickHandler}>
        <ContentCard>
          <div className={styles.connectorCardWrapper}>
            <ConnectorCard
              icon={icon}
              releaseStage={releaseStage}
              connectionName={formatMessage({ id: "destinations.dontHaveYourOwnDestination" })}
              connectorName={formatMessage({ id: "destinations.startWith" }, { name })}
              fullWidth
            />
          </div>
        </ContentCard>
      </button>
    </div>
  );
};
