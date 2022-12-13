import { faWarning } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage } from "react-intl";

import { RotateIcon } from "components/icons/RotateIcon";
import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectorBuilderState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { useBuilderErrors } from "../useBuilderErrors";
import styles from "./StreamTestButton.module.scss";

interface StreamTestButtonProps {
  readStream: () => void;
}

export const StreamTestButton: React.FC<StreamTestButtonProps> = ({ readStream }) => {
  const { editorView, yamlIsValid, testStreamIndex } = useConnectorBuilderState();
  const { validateAndTouch } = useBuilderErrors();

  const handleClick = () => {
    if (editorView === "yaml") {
      readStream();
      return;
    }

    validateAndTouch(readStream, ["global", testStreamIndex]);
  };

  const buttonDisabled = editorView === "yaml" && !yamlIsValid;
  const tooltipContent = buttonDisabled ? <FormattedMessage id="connectorBuilder.invalidYamlTest" /> : null;

  const testButton = (
    <Button
      className={styles.testButton}
      size="sm"
      onClick={handleClick}
      disabled={buttonDisabled}
      icon={
        buttonDisabled ? (
          <FontAwesomeIcon icon={faWarning} />
        ) : (
          <div>
            <RotateIcon width={styles.testIconHeight} height={styles.testIconHeight} />
          </div>
        )
      }
    >
      <Text className={styles.testButtonText} size="sm" bold>
        <FormattedMessage id="connectorBuilder.testButton" />
      </Text>
    </Button>
  );

  return buttonDisabled ? (
    <Tooltip control={testButton} containerClassName={styles.testButtonTooltipContainer}>
      {tooltipContent}
    </Tooltip>
  ) : (
    testButton
  );
};
