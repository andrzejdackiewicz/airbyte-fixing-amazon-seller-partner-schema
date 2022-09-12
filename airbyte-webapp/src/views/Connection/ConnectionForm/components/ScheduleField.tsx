import { Field, FieldInputProps, FieldProps, FormikProps } from "formik";
import { ChangeEvent, useMemo } from "react";
import { useIntl } from "react-intl";

import { ControlLabels, DropDown, DropDownRow, Input } from "components";

import { ConnectionScheduleData, ConnectionScheduleType } from "core/request/AirbyteClient";

import availableCronTimeZones from "../../../../config/availableCronTimeZones.json";
import { ConnectionFormMode } from "../ConnectionForm";
import { FormikConnectionFormValues, useFrequencyDropdownData } from "../formConfig";
import styles from "./ScheduleField.module.scss";

interface ScheduleFieldProps {
  scheduleData: ConnectionScheduleData | undefined;
  mode: ConnectionFormMode;
  onDropDownSelect?: (item: DropDownRow.IDataItem) => void;
}

const CRON_DEFAULT_VALUE = {
  cronTimeZone: "UTC",
  // Fire at 12:00 PM (noon) every day
  cronExpression: "0 0 12 * * ?",
};

const ScheduleField: React.FC<ScheduleFieldProps> = ({ scheduleData, mode, onDropDownSelect }) => {
  const { formatMessage } = useIntl();
  const frequencies = useFrequencyDropdownData(scheduleData);

  const onScheduleChange = (item: DropDownRow.IDataItem, form: FormikProps<FormikConnectionFormValues>) => {
    onDropDownSelect?.(item);

    let scheduleData: ConnectionScheduleData;
    const isManual = item.value === ConnectionScheduleType.manual;
    const isCron = item.value === ConnectionScheduleType.cron;

    // Set scheduleType for yup validation
    const scheduleType = isManual || isCron ? (item.value as ConnectionScheduleType) : ConnectionScheduleType.basic;

    // Set scheduleData.basicSchedule
    if (isManual || isCron) {
      scheduleData = {
        basicSchedule: undefined,
        cron: isCron ? CRON_DEFAULT_VALUE : undefined,
      };
    } else {
      scheduleData = {
        basicSchedule: item.value,
      };
    }

    form.setValues(
      {
        ...form.values,
        scheduleType,
        scheduleData,
      },
      true
    );
  };

  const getBasicScheduleValue = (value: ConnectionScheduleData, form: FormikProps<FormikConnectionFormValues>) => {
    const { scheduleType } = form.values;

    if (scheduleType === ConnectionScheduleType.basic) {
      return value.basicSchedule;
    }

    return formatMessage({
      id: `frequency.${scheduleType}`,
    }).toLowerCase();
  };

  const getZoneValue = (currentSelectedZone = "UTC") => currentSelectedZone;

  const onCronChange = (
    event: DropDownRow.IDataItem | ChangeEvent<HTMLInputElement>,
    field: FieldInputProps<ConnectionScheduleData>,
    form: FormikProps<FormikConnectionFormValues>,
    key: string
  ) => {
    form.setFieldValue(field.name, {
      cron: {
        ...field.value?.cron,
        [key]: (event as DropDownRow.IDataItem).value
          ? (event as DropDownRow.IDataItem).value
          : (event as ChangeEvent<HTMLInputElement>).currentTarget.value,
      },
    });
  };

  const cronTimeZones = useMemo(() => {
    return availableCronTimeZones.map((zone: string) => ({ label: zone, value: zone }));
  }, []);

  const isCron = (form: FormikProps<FormikConnectionFormValues>): boolean => {
    return form.values.scheduleType === ConnectionScheduleType.cron;
  };

  return (
    <Field name="scheduleData">
      {({ field, meta, form }: FieldProps<ConnectionScheduleData>) => (
        <>
          <div className={styles.flexRow}>
            <div className={styles.leftFieldCol}>
              <ControlLabels
                className={styles.connectorLabel}
                nextLine
                error={!!meta.error && meta.touched}
                label={formatMessage({
                  id: "form.frequency",
                })}
                message={formatMessage({
                  id: "form.frequency.message",
                })}
              />
            </div>
            <div className={styles.rightFieldCol} style={{ pointerEvents: mode === "readonly" ? "none" : "auto" }}>
              <DropDown
                {...field}
                error={!!meta.error && meta.touched}
                options={frequencies}
                onChange={(item) => {
                  onScheduleChange(item, form);
                }}
                value={getBasicScheduleValue(field.value, form)}
              />
            </div>
          </div>
          {isCron(form) && (
            <div className={styles.flexRow}>
              <div className={styles.leftFieldCol}>
                <ControlLabels
                  className={styles.connectorLabel}
                  nextLine
                  error={!!meta.error && meta.touched}
                  label={formatMessage({
                    id: "form.cronExpression",
                  })}
                  message={formatMessage({
                    id: "form.cronExpression.message",
                  })}
                />
              </div>

              <div className={styles.rightFieldCol} style={{ pointerEvents: mode === "readonly" ? "none" : "auto" }}>
                <div className={styles.flexRow}>
                  <Input
                    disabled={mode === "readonly"}
                    error={!!meta.error}
                    data-testid="cronExpression"
                    placeholder={formatMessage({
                      id: "form.cronExpression.placeholder",
                    })}
                    value={field.value?.cron?.cronExpression}
                    onChange={(event: ChangeEvent<HTMLInputElement>) =>
                      onCronChange(event, field, form, "cronExpression")
                    }
                  />
                  <DropDown
                    className={styles.cronZonesDropdown}
                    options={cronTimeZones}
                    value={getZoneValue(field.value?.cron?.cronTimeZone)}
                    onChange={(item: DropDownRow.IDataItem) => onCronChange(item, field, form, "cronTimeZone")}
                  />
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </Field>
  );
};

export default ScheduleField;
