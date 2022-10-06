import React from "react";
import { useIntl } from "react-intl";

import { ConnectorCard } from "components";
import { SlickSlider } from "components/ui/SlickSlider";

import { DestinationConnectorCard } from "../../types";
import styles from "./FrequentlyUsedDestinationsCard.module.scss";

export interface FrequentlyUsedDestinationsCardProps {
  destinations: DestinationConnectorCard[];
  onDestinationSelect: (id: string) => void;
}

export const FrequentlyUsedDestinationsCard: React.FC<FrequentlyUsedDestinationsCardProps> = ({
  destinations,
  onDestinationSelect,
}) => {
  const { formatMessage } = useIntl();

  if (!destinations?.length) {
    return null;
  }
  const onSlideClick = (id: string) => {
    onDestinationSelect(id);
  };
  return (
    <div className={styles.container}>
      <SlickSlider title={formatMessage({ id: "destinations.frequentlyUsed" })}>
        {destinations.map(({ destinationDefinitionId, name, icon, releaseStage }, index) => (
          <button key={index} className={styles.card} onClick={() => onSlideClick(destinationDefinitionId)}>
            <ConnectorCard connectionName={name} icon={icon} releaseStage={releaseStage} fullWidth />
          </button>
        ))}
      </SlickSlider>
    </div>
  );
};
