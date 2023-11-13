import { Box, IconButton, Tooltip } from "@mui/material";
import queryString from "query-string";
import React, { useCallback } from "react";
import { FormattedMessage } from "react-intl";
import { CellProps } from "react-table";
import styled from "styled-components";

import { DisabledIcon } from "components/icons/DisabledIcon";
import { FailedIcon } from "components/icons/FailedIcon";
import { GreenIcon } from "components/icons/GreenIcon";
import { GreenLoaderIcon } from "components/icons/GreenLoaderIcon";
import { WaitingIcon } from "components/icons/WaitingIcon";
import { LabeledSwitch } from "components/LabeledSwitch";
import Table from "components/Table";

import { FeatureItem, useFeature } from "hooks/services/Feature";
import useRouter from "hooks/useRouter";

import ConnectionSettingsCell from "./components/ConnectionSettingsCell";
import LastSyncCell from "./components/LastSyncCell";
import NameCell from "./components/NameCell";
import NewTabIconButton from "./components/NewTabIconButton";
import { ITableDataItem, SortOrderEnum } from "./types";
import { RoutePaths } from "../../pages/routePaths";
const SwitchContent = styled.div`
  display: flex;
  align-items: center;
`;

const HeaderColumns = styled.div`
  display: flex;
  flex-wrap: nowrap;
  min-width: 100px;
`;

const NameColums = styled.div`
  display: flex;
  aligin-items: center;
`;

interface IProps {
  data: ITableDataItem[];
  entity: "source" | "destination" | "connection";
  onClickRow?: (data: ITableDataItem) => void;
  onChangeStatus?: (id: string, status: string | undefined) => void;
  onSync?: (id: string) => void;
  rowId?: string;
  statusLoading?: boolean;
  switchSize?: string;
}
//
const ConnectionTable: React.FC<IProps> = ({ data, entity, onChangeStatus, onSync, rowId, statusLoading }) => {
  const { query, push } = useRouter();
  const allowSync = useFeature(FeatureItem.AllowSync);

  const sortBy = query.sortBy || "entityName";
  const sortOrder = query.order || SortOrderEnum.ASC;

  const onSortClick = useCallback(
    (field: string) => {
      const order =
        sortBy !== field ? SortOrderEnum.ASC : sortOrder === SortOrderEnum.ASC ? SortOrderEnum.DESC : SortOrderEnum.ASC;
      push({
        search: queryString.stringify(
          {
            sortBy: field,
            order,
          },
          { skipNull: true }
        ),
      });
    },
    [push, sortBy, sortOrder]
  );

  const onClickRows = (connectionId: string) => push(`/${RoutePaths.Connections}/${connectionId}`);

  const columns = React.useMemo(
    () => [
      onChangeStatus
        ? {
            Header: "",
            accessor: "lastSyncStatus",
            customWidth: 1,
            Cell: ({ cell }: CellProps<ITableDataItem>) => {
              return (
                <SwitchContent
                  onClick={(e) => {
                    onChangeStatus(cell.row.original.connectionId, cell.row.original.status);
                    e.preventDefault();
                  }}
                >
                  <LabeledSwitch
                    swithSize="medium"
                    id={`${cell.row.original.connectionId}`}
                    checked={cell.row.original.status === "Active" ? true : false}
                    loading={rowId === cell.row.original.connectionId && statusLoading ? true : false}
                  />
                </SwitchContent>
              );
            },
          }
        : {
            Header: "Sync Status",
            accessor: "latestSyncJobStatus",
            Cell: ({ cell }: CellProps<ITableDataItem>) => {
              return cell.row.original.latestSyncJobStatus === "succeeded" ? (
                <Box pl={3}>
                  {" "}
                  <Tooltip title={<FormattedMessage id="sync.successful" />} placement="top">
                    <IconButton>
                      <GreenIcon />
                    </IconButton>
                  </Tooltip>
                </Box>
              ) : cell.row.original.latestSyncJobStatus === "running" ? (
                <Box pl={3}>
                  <Tooltip title={<FormattedMessage id="sync.running" />} placement="top">
                    <IconButton>
                      <GreenLoaderIcon />
                    </IconButton>
                  </Tooltip>
                </Box>
              ) : cell.row.original.latestSyncJobStatus === "failed" ? (
                <Box pl={3}>
                  <Tooltip title={<FormattedMessage id="sync.failed" />} placement="top">
                    <IconButton>
                      <FailedIcon />
                    </IconButton>
                  </Tooltip>
                </Box>
              ) : cell.row.original.latestSyncJobStatus === "waiting" ? (
                <Box pl={3}>
                  <Tooltip title={<FormattedMessage id="sync.waiting" />} placement="top">
                    <IconButton>
                      <WaitingIcon />
                    </IconButton>
                  </Tooltip>
                </Box>
              ) : cell.row.original.latestSyncJobStatus === "disabled" ? (
                <Box pl={3}>
                  <Tooltip title={<FormattedMessage id="sync.disabled" />} placement="top">
                    <IconButton>
                      <DisabledIcon />
                    </IconButton>
                  </Tooltip>
                </Box>
              ) : null;
            },
          },
      {
        Header: <FormattedMessage id="tables.name" />,
        headerHighlighted: true,
        accessor: "name",
        customWidth: 30,
        Cell: ({ cell }: CellProps<ITableDataItem>) => {
          return (
            <NameColums>
              <NameCell value={cell.value} onClickRow={() => onClickRows(cell.row.original.connectionId)} />
              <NewTabIconButton id={cell.row.original.connectionId} type="Connections" />
            </NameColums>
          );
        },
      },
      // {
      //   Header: <FormattedMessage id="tables.status" />,
      //   accessor: "statusLang",
      // },
      {
        Header: <FormattedMessage id="tables.status" />,
        accessor: "status",
        Cell: ({ cell }: CellProps<ITableDataItem>) => {
          return cell.row.original.status === "active" ? (
            <FormattedMessage id="connection.active" />
          ) : (
            <FormattedMessage id="connection.inactive" />
          );
        },
      },
      {
        Header: (
          <HeaderColumns>
            <FormattedMessage id="tables.lastSyncAt" />
          </HeaderColumns>
        ),
        accessor: "latestSyncJobCreatedAt",
        Cell: ({ cell, row }: CellProps<ITableDataItem>) => (
          <LastSyncCell timeInSecond={cell.value} enabled={row.original.enabled} />
        ),
      },
      {
        Header: <FormattedMessage id="tables.destination" />,
        headerHighlighted: true,
        accessor: "entityName",
      },
      {
        Header: <FormattedMessage id="tables.source" />,
        accessor: "connectorName",
      },
      {
        Header: "",
        accessor: "connectionId",
        customWidth: 1,
        Cell: ({ cell }: CellProps<ITableDataItem>) => {
          return (
            <ConnectionSettingsCell
              id={cell.value}
              onClick={() => {
                onClickRows(cell.value);
              }}
            />
          );
        },
      },
    ],
    [allowSync, entity, onChangeStatus, onSync, onSortClick, sortBy, sortOrder]
  );

  return <Table columns={columns} data={data} erroredRows />;
};

export default ConnectionTable;
