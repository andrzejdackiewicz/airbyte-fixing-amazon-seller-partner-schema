import classNames from "classnames";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { DropDownOptionDataItem } from "components/ui/DropDown";
import { PillSelect, PillButtonVariant } from "components/ui/PillSelect";
import { PillButtonVariant } from "components/ui/PillSelect/PillButton";

import { DestinationSyncMode, SyncMode } from "core/request/AirbyteClient";

import styles from "./SyncModeSelect.module.scss";

interface SyncModeValue {
  syncMode: SyncMode;
  destinationSyncMode: DestinationSyncMode;
}

export interface SyncModeOption {
  value: SyncModeValue;
}

interface SyncModeSelectProps {
  className?: string;
  onChange?: (option: DropDownOptionDataItem<SyncModeValue>) => void;
  options: SyncModeOption[];
  value: Partial<SyncModeValue>;
  variant?: PillButtonVariant;
  variant?: PillButtonVariant;
}

export const SyncModeSelect: React.FC<SyncModeSelectProps> = ({
  className,
  variant,
  options,
  onChange,
  value,
  variant,
}) => {
  const pillSelectOptions = useMemo(() => {
    return options.map(({ value }) => {
      const { syncMode, destinationSyncMode } = value;
      return {
        label: (
          <>
            <FormattedMessage id={`syncMode.${syncMode}`} />
            {` | `}
            <FormattedMessage id={`destinationSyncMode.${destinationSyncMode}`} />
          </>
        ),
        value,
      };
    });
  }, [options]);

  return (
    <PillSelect
      variant={variant}
      options={pillSelectOptions}
      value={value}
      onChange={onChange}
      className={classNames(styles.pillSelect, className)}
      variant={variant}
    />
  );
};
