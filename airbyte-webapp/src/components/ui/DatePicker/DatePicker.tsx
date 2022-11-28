import { faCalendarAlt } from "@fortawesome/free-regular-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import en from "date-fns/locale/en-US";
import dayjs from "dayjs";
import React, { useCallback, useEffect, useMemo, useRef } from "react";
import ReactDatePicker, { registerLocale } from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import { useIntl } from "react-intl";

import { Button } from "../Button";
import { Input } from "../Input";
import styles from "./DatePicker.module.scss";

/**
 * Converts a UTC string into a JS Date object with the same local time
 *
 * Necessary because react-datepicker does not allow us to set the timezone to UTC, only the current browser time.
 * In order to display the UTC timezone in the datepicker, we need to convert it into the local time:
 *
 * 2022-01-01T09:00:00Z       - the UTC format that airbyte-server expects (e.g. 9:00am)
 * 2022-01-01T10:00:00+01:00  - what react-datepicker might convert this date into and display (e.g. 10:00am - bad!)
 * 2022-01-01T09:00:00+01:00  - what we give react-datepicker instead, to trick it (User sees 9:00am - good!)
 */

// TODO: accept a string instead here
export const toEquivalentLocalTime = (input: string): Date | undefined => {
  if (!input) {
    return undefined;
  }

  const date = dayjs.utc(input);

  if (!date?.isValid()) {
    return undefined;
  }

  // Get the user's UTC offset based on the local time
  const browserUtcOffset = dayjs().utcOffset();

  // Convert the selected date into a string which we can use to initialize a new date object.
  // The second parameter to utcOffset() keeps the same local time, only changing the timezone.
  const dateInUtcAsString = date.utcOffset(browserUtcOffset, true).format();

  const equivalentDate = dayjs(dateInUtcAsString);

  // dayjs does not 0-pad years when formatting, so it's possible to have an invalid date here
  // https://github.com/iamkun/dayjs/issues/1745
  if (!equivalentDate.isValid()) {
    return undefined;
  }

  return equivalentDate.toDate();
};

export interface DatePickerProps {
  error?: boolean;
  value: string;
  onChange: (value: string) => void;
  withTime?: boolean;
  disabled?: boolean;
  onBlur?: (ev: React.FocusEvent<HTMLInputElement>) => void;
  placeholder?: string;
}

interface DatePickerButtonTriggerProps {
  onClick?: () => void;
}

const DatepickerButton = React.forwardRef<HTMLButtonElement, DatePickerButtonTriggerProps>(({ onClick }, ref) => {
  const { formatMessage } = useIntl();

  return (
    <Button
      className={styles.datepickerButton}
      aria-label={formatMessage({ id: "form.openDatepicker" })}
      onClick={onClick}
      ref={ref}
      type="button"
      variant="clear"
      icon={<FontAwesomeIcon icon={faCalendarAlt} className={styles.dropdownButton} fixedWidth />}
    />
  );
});

export const DatePicker: React.FC<DatePickerProps> = ({
  disabled,
  error,
  onChange,
  onBlur,
  placeholder,
  value = "",
  withTime = false,
}) => {
  const { locale } = useIntl();
  const datepickerRef = useRef<ReactDatePicker>(null);

  // Additional locales can be registered here as necessary
  useEffect(() => {
    switch (locale) {
      case "en":
        registerLocale(locale, en);
        break;
    }
  }, [locale]);

  const handleDatepickerChange = useCallback(
    (val: Date | null) => {
      const date = dayjs(val);
      if (!date.isValid()) {
        onChange("");
        return;
      }

      const formattedDate = withTime ? date.utcOffset(0, true).format() : date.format("YYYY-MM-DD");
      onChange(formattedDate);
    },
    [onChange, withTime]
  );

  const handleInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      onChange(e.target.value);
    },
    [onChange]
  );

  const localDate = useMemo(() => toEquivalentLocalTime(value), [value]);

  return (
    <div className={styles.wrapper}>
      <Input
        placeholder={placeholder}
        error={error}
        value={value}
        onChange={handleInputChange}
        onBlur={onBlur}
        onFocus={() => datepickerRef.current?.setOpen(true)}
      />
      <div className={styles.datepickerButtonContainer}>
        <ReactDatePicker
          ref={datepickerRef}
          showPopperArrow={false}
          showTimeSelect={withTime}
          disabled={disabled}
          locale={locale}
          selected={localDate}
          onChange={handleDatepickerChange}
          onBlur={onBlur}
          value={undefined}
          customInput={<DatepickerButton />}
          popperClassName={styles.popup}
        />
      </div>
    </div>
  );
};
