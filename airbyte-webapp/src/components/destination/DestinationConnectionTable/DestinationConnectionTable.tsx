import React from "react";
import { useNavigate } from "react-router-dom";

import { ConnectionTable } from "components/EntityTable";
import { ITableDataItem } from "components/EntityTable/types";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem } from "core/request/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

import styles from "./DestinationConnectionTable.module.scss";

interface IProps {
  connections: WebBackendConnectionListItem[];
}

export const DestinationConnectionTable: React.FC<IProps> = ({ connections }) => {
  const navigate = useNavigate();

  const data = getConnectionTableData(connections, "destination");

  const clickRow = (source: ITableDataItem) => navigate(`../../../${RoutePaths.Connections}/${source.connectionId}`);

  return (
    <div className={styles.content}>
      <ConnectionTable data={data} onClickRow={clickRow} entity="destination" />
    </div>
  );
};
