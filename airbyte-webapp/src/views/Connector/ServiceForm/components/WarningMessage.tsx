import React from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { useConfig } from "config";
import { ReleaseStage } from "core/request/AirbyteClient";

import styles from "./WarningMessage.module.scss";

interface WarningMessageProps {
  stage: typeof ReleaseStage.alpha | typeof ReleaseStage.beta;
}

export const WarningMessage: React.FC<WarningMessageProps> = ({ stage }) => {
  const config = useConfig();
  return (
    <div className={styles.container}>
      <Text size="sm">
        <FormattedMessage id={`connector.releaseStage.${stage}.description`} />{" "}
        <FormattedMessage
          id="connector.connectorsInDevelopment.docLink"
          values={{
            lnk: (node: React.ReactNode) => (
              <a className={styles.link} href={config.links.productReleaseStages} target="_blank" rel="noreferrer">
                {node}
              </a>
            ),
          }}
        />
      </Text>
    </div>
  );
};
