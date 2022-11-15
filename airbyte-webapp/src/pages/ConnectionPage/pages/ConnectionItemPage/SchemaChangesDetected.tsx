import classNames from "classnames";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";

import { SchemaChange } from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "views/Connection/ConnectionForm/components/refreshSourceSchemaWithConfirmationOnDirty";

import { ConnectionSettingsRoutes } from "./ConnectionSettingsRoutes";
import styles from "./SchemaChangesDetected.module.scss";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

export const useSchemaChanges = (schemaChange: SchemaChange) => {
  const isSchemaChangesFeatureEnabled = process.env.REACT_APP_AUTO_DETECT_SCHEMA_CHANGES === "true";

  return useMemo(() => {
    const hasSchemaChanges = isSchemaChangesFeatureEnabled && schemaChange !== SchemaChange.no_change;
    const hasBreakingSchemaChange = hasSchemaChanges && schemaChange === SchemaChange.breaking;
    const hasNonBreakingSchemaChange = hasSchemaChanges && schemaChange === SchemaChange.non_breaking;

    return {
      schemaChange,
      hasSchemaChanges,
      hasBreakingSchemaChange,
      hasNonBreakingSchemaChange,
    };
  }, [isSchemaChangesFeatureEnabled, schemaChange]);
};

export const SchemaChangesDetected: React.FC = () => {
  const {
    connection: { schemaChange },
    schemaRefreshing,
    schemaHasBeenRefreshed,
  } = useConnectionEditService();
  const { hasFormChanges } = useFormChangeTrackerService();

  const { hasBreakingSchemaChange, hasNonBreakingSchemaChange } = useSchemaChanges(schemaChange);
  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(hasFormChanges);
  const location = useLocation();
  const navigate = useNavigate();

  if (schemaHasBeenRefreshed) {
    return null;
  }

  const onReviewActionButtonClick: React.MouseEventHandler<HTMLButtonElement> = () => {
    if (!location.pathname.includes(`/${ConnectionSettingsRoutes.REPLICATION}`)) {
      navigate(ConnectionSettingsRoutes.REPLICATION);
    }

    refreshSchema();
  };

  const schemaChangeClassNames = {
    [styles.breaking]: hasBreakingSchemaChange,
    [styles.nonBreaking]: hasNonBreakingSchemaChange,
  };

  return (
    <div className={classNames(styles.container, schemaChangeClassNames)}>
      <Text size="lg">
        <FormattedMessage id={`connection.schemaChange.${hasBreakingSchemaChange ? "breaking" : "nonBreaking"}`} />
      </Text>
      <Button onClick={onReviewActionButtonClick} isLoading={schemaRefreshing}>
        <FormattedMessage id="connection.schemaChange.reviewAction" />
      </Button>
    </div>
  );
};
