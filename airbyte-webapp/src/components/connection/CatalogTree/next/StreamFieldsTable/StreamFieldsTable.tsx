import { faArrowRight } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { createColumnHelper } from "@tanstack/react-table";
import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { SyncSchemaField, SyncSchemaFieldObject } from "core/domain/catalog";
import { AirbyteStreamConfiguration } from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useDestinationDefinition } from "services/connector/DestinationDefinitionService";
import { useSourceDefinition } from "services/connector/SourceDefinitionService";
import { getIcon } from "utils/imageUtils";
import { equal } from "utils/objects";
import { getDataType } from "utils/useTranslateDataType";

import { CheckBox } from "../../../../ui/CheckBox";
import { RadioButton } from "../../../../ui/RadioButton";
import { pathDisplayName } from "../../PathPopout";
import { NextTable } from "../NextTable";
import styles from "./StreamFieldsTable.module.scss";

export interface StreamFieldsTableProps {
  config?: AirbyteStreamConfiguration;
  onCursorSelect: (cursorPath: string[]) => void;
  onPkSelect: (pkPath: string[]) => void;
  shouldDefinePk: boolean;
  shouldDefineCursor: boolean;
  syncSchemaFields: SyncSchemaField[];
}

// copied from StreamConnectionHeader
const renderIcon = (icon?: string): JSX.Element => <div className={styles.icon}>{getIcon(icon)}</div>;

export const StreamFieldsTable: React.FC<StreamFieldsTableProps> = ({
  config,
  onPkSelect,
  onCursorSelect,
  shouldDefineCursor,
  shouldDefinePk,
  syncSchemaFields,
}) => {
  const { formatMessage } = useIntl();

  const isCursor = useMemo(() => (path: string[]) => equal(config?.cursorField, path), [config?.cursorField]);
  const isPrimaryKey = useMemo(
    () => (path: string[]) => !!config?.primaryKey?.some((p) => equal(p, path)),
    [config?.primaryKey]
  );

  // header group icons:
  const {
    connection: { source, destination },
  } = useConnectionEditService();
  const sourceDefinition = useSourceDefinition(source.sourceDefinitionId);
  const destinationDefinition = useDestinationDefinition(destination.destinationDefinitionId);

  // prepare data for table
  interface Stream {
    path: string[];
    dataType: string;
    cursorDefined?: boolean;
    primaryKeyDefined?: boolean;
  }
  const tableData: Stream[] = syncSchemaFields.map((stream) => ({
    path: stream.path,
    dataType: getDataType(stream),
    cursorDefined: shouldDefineCursor && SyncSchemaFieldObject.isPrimitive(stream),
    primaryKeyDefined: shouldDefinePk && SyncSchemaFieldObject.isPrimitive(stream),
  }));

  const columnHelper = createColumnHelper<Stream>();

  const sourceColumns = [
    columnHelper.accessor("path", {
      id: "sourcePath",
      header: () => <FormattedMessage id="form.field.name" />,
      cell: ({ getValue }) => pathDisplayName(getValue()),
      meta: {
        thClassName: styles.headerCell,
        tdClassName: styles.textCell,
      },
    }),
    columnHelper.accessor("dataType", {
      id: "sourceDataType",
      header: () => <FormattedMessage id="form.field.dataType" />,
      cell: ({ getValue }) => (
        <FormattedMessage id={`${getValue()}`} defaultMessage={formatMessage({ id: "airbyte.datatype.unknown" })} />
      ),
      meta: {
        thClassName: styles.headerCell,
        tdClassName: styles.dataTypeCell,
      },
    }),
    columnHelper.accessor("cursorDefined", {
      id: "sourceCursorDefined",
      header: () => <>{shouldDefineCursor && <FormattedMessage id="form.field.cursorField" />}</>,
      cell: ({ getValue, row }) => {
        return (
          getValue() && (
            <RadioButton checked={isCursor(row.original.path)} onChange={() => onCursorSelect(row.original.path)} />
          )
        );
      },
      meta: {
        thClassName: styles.headerCell,
        tdClassName: styles.checkboxCell,
      },
    }),
    columnHelper.accessor("primaryKeyDefined", {
      id: "sourcePrimaryKeyDefined",
      header: () => shouldDefinePk && <FormattedMessage id="form.field.primaryKey" />,
      cell: ({ getValue, row }) => {
        return (
          getValue() && (
            <CheckBox checked={isPrimaryKey(row.original.path)} onChange={() => onPkSelect(row.original.path)} />
          )
        );
      },
      meta: {
        thClassName: styles.headerCell,
        tdClassName: styles.textCell,
      },
    }),
  ];

  const destinationColumns = [
    columnHelper.accessor("path", {
      id: "destinationPath",
      header: () => <FormattedMessage id="form.field.name" />,
      cell: ({ getValue }) => pathDisplayName(getValue()),
      meta: {
        thClassName: styles.headerCell,
        tdClassName: styles.textCell,
      },
    }),
    // In the design, but we may be unable to get the destination data type
    columnHelper.accessor("dataType", {
      id: "destinationDataType",
      header: () => <FormattedMessage id="form.field.dataType" />,
      cell: ({ getValue }) => (
        <FormattedMessage id={`${getValue()}`} defaultMessage={formatMessage({ id: "airbyte.datatype.unknown" })} />
      ),
      meta: {
        thClassName: styles.headerCell,
        tdClassName: styles.dataTypeCell,
      },
    }),
  ];

  const columns = [
    columnHelper.group({
      id: "source",
      header: () => <span className={styles.connectorIconContainer}>{renderIcon(sourceDefinition.icon)} Source </span>,
      columns: sourceColumns,
      meta: {
        thClassName: styles.headerGroupCell,
      },
    }),
    columnHelper.group({
      id: "arrow",
      header: () => <FontAwesomeIcon icon={faArrowRight} />,
      columns: [
        {
          id: "_", // leave the column name empty
          cell: () => <FontAwesomeIcon icon={faArrowRight} />,
          meta: {
            thClassName: styles.headerCell,
            tdClassName: styles.arrowCell,
          },
        },
      ],
      meta: {
        thClassName: styles.headerGroupCell,
      },
    }),
    columnHelper.group({
      id: "destination",
      header: () => (
        <span className={styles.connectorIconContainer}>{renderIcon(destinationDefinition.icon)} Destination</span>
      ),
      columns: destinationColumns,
      meta: {
        thClassName: styles.headerGroupCell,
        tdClassName: styles.bodyCell,
      },
    }),
  ];

  return <NextTable<Stream> columns={columns} data={tableData} />;
};
