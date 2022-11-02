import React from "react";

import { DeleteBlock } from "components/common/DeleteBlock";
import { UpdateConnectionGeography } from "components/UpdateConnectionGeography";

import { PageTrackingCodes, useTrackPage } from "hooks/services/Analytics";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { useAdvancedModeSetting } from "hooks/services/useAdvancedModeSetting";
import { useDeleteConnection } from "hooks/services/useConnectionHook";

import styles from "./ConnectionSettingsTab.module.scss";
import { StateBlock } from "./StateBlock";

export const ConnectionSettingsTab: React.FC = () => {
  const { connection } = useConnectionEditService();
  const { mutateAsync: deleteConnection } = useDeleteConnection();
  const canUpdateDefaultDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);

  const [isAdvancedMode] = useAdvancedModeSetting();
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_SETTINGS);
  const onDelete = () => deleteConnection(connection);

  return (
    <div className={styles.container}>
      {canUpdateDefaultDataResidency && <UpdateConnectionGeography />}
      {isAdvancedMode && <StateBlock connectionId={connection.connectionId} />}
      <DeleteBlock type="connection" onDelete={onDelete} />
    </div>
  );
};
