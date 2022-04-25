import queryString from "query-string";
import React, { useCallback } from "react";
import { FormattedMessage } from "react-intl";
import { CellProps } from "react-table";
import styled from "styled-components";

import Table from "components/Table";

import { FeatureItem, useFeatureService } from "hooks/services/Feature";
import useRouter from "hooks/useRouter";

import ConnectionSettingsCell from "./components/ConnectionSettingsCell";
import ConnectorCell from "./components/ConnectorCell";
import FrequencyCell from "./components/FrequencyCell";
import LastSyncCell from "./components/LastSyncCell";
import NameCell from "./components/NameCell";
import SortButton from "./components/SortButton";
import StatusCell from "./components/StatusCell";
import { ITableDataItem, SortOrderEnum } from "./types";

const Content = styled.div`
  margin: 0 32px 0 27px;
`;

type IProps = {
  data: ITableDataItem[];
  entity: "source" | "destination" | "connection";
  onClickRow?: (data: ITableDataItem) => void;
  onChangeStatus: (id: string) => void;
  onSync: (id: string) => void;
};

const ConnectionTable: React.FC<IProps> = ({ data, entity, onClickRow, onChangeStatus, onSync }) => {
  const { query, push } = useRouter();
  const { hasFeature } = useFeatureService();
  const allowSync = hasFeature(FeatureItem.AllowSync);

  const sortBy = query.sortBy || "entity";
  const sortOrder = query.order || SortOrderEnum.ASC;

  const onSortClick = useCallback(
    (field: string) => {
      const order =
        sortBy !== field ? SortOrderEnum.ASC : sortOrder === SortOrderEnum.ASC ? SortOrderEnum.DESC : SortOrderEnum.ASC;
      push({
        search: queryString.stringify(
          {
            sortBy: field,
            order: order,
          },
          { skipNull: true }
        ),
      });
    },
    [push, sortBy, sortOrder]
  );

  const sortData = useCallback(
    (a, b) => {
      let result;
      if (sortBy === "lastSync") {
        result = b[sortBy] - a[sortBy];
      } else {
        result = a[`${sortBy}Name`].toLowerCase().localeCompare(b[`${sortBy}Name`].toLowerCase());
      }

      if (sortOrder === SortOrderEnum.DESC) {
        return -1 * result;
      }

      return result;
    },
    [sortBy, sortOrder]
  );

  const sortingData = React.useMemo(() => data.sort(sortData), [sortData, data]);
  console.log(sortingData);
  const columns = React.useMemo(
    () => [
      {
        Header: (
          <>
            {entity === "connection" ? (
              <FormattedMessage id="tables.destinationConnectionToName" />
            ) : (
              <FormattedMessage id={`tables.${entity}ConnectionToName`} />
            )}
            <SortButton
              wasActive={sortBy === "entity"}
              lowToLarge={sortOrder === SortOrderEnum.ASC}
              onClick={() => onSortClick("entity")}
            />
          </>
        ),
        headerHighlighted: true,
        accessor: "entityName",
        customWidth: 40,
        Cell: ({ cell, row }: CellProps<ITableDataItem>) => (
          <NameCell
            value={cell.value}
            enabled={row.original.enabled}
            status={row.original.lastSyncStatus}
            icon={entity === "connection"}
            img={row.original.entityIcon}
          />
        ),
      },
      {
        Header: (
          <>
            {entity === "connection" ? (
              <FormattedMessage id="tables.sourceConnectionToName" />
            ) : (
              <FormattedMessage id="tables.connector" />
            )}
            <SortButton
              wasActive={sortBy === "connector"}
              lowToLarge={sortOrder === SortOrderEnum.ASC}
              onClick={() => onSortClick("connector")}
            />
          </>
        ),
        accessor: "connectorName",
        Cell: ({ cell, row }: CellProps<ITableDataItem>) => (
          <ConnectorCell value={cell.value} enabled={row.original.enabled} img={row.original.connectorIcon} />
        ),
      },

      {
        Header: <FormattedMessage id="tables.frequency" />,
        accessor: "schedule",
        Cell: ({ cell, row }: CellProps<ITableDataItem>) => (
          <FrequencyCell value={cell.value} enabled={row.original.enabled} />
        ),
      },
      {
        Header: (
          <>
            <FormattedMessage id="tables.lastSync" />
            <SortButton
              wasActive={sortBy === "lastSync"}
              lowToLarge={sortOrder === SortOrderEnum.ASC}
              onClick={() => onSortClick("lastSync")}
            />
          </>
        ),
        accessor: "lastSync",
        Cell: ({ cell, row }: CellProps<ITableDataItem>) => (
          <LastSyncCell timeInSecond={cell.value} enabled={row.original.enabled} />
        ),
      },
      {
        Header: <FormattedMessage id="tables.enabled" />,
        accessor: "enabled",
        customWidth: 1,
        Cell: ({ cell, row }: CellProps<ITableDataItem>) => (
          <StatusCell
            enabled={cell.value}
            id={row.original.connectionId}
            isSyncing={row.original.isSyncing}
            isManual={!row.original.schedule}
            onChangeStatus={onChangeStatus}
            onSync={onSync}
            allowSync={allowSync}
          />
        ),
      },
      {
        Header: "",
        accessor: "connectionId",
        customWidth: 1,
        Cell: ({ cell }: CellProps<ITableDataItem>) => <ConnectionSettingsCell id={cell.value} />,
      },
    ],
    [allowSync, entity, onChangeStatus, onSync, onSortClick, sortBy, sortOrder]
  );

  return (
    <Content>
      <Table columns={columns} data={sortingData} onClickRow={onClickRow} erroredRows />
    </Content>
  );
};

export default ConnectionTable;
