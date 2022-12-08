import React, { useState } from "react";
import styled from "styled-components";

import { LoadingPage } from "components";

import useRouter from "hooks/useRouter";
import { UnauthorizedModal } from "pages/ConnectionPage/pages/AllConnectionsPage/components/UnauthorizedModal";
import { UpgradePlanBar } from "pages/ConnectionPage/pages/AllConnectionsPage/components/UpgradePlanBar";
import { RoutePaths } from "pages/routePaths";
import { ResourceNotFoundErrorBoundary } from "views/common/ResorceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import SideBar from "views/layout/SideBar";

const MainContainer = styled.div`
  width: 100%;
  height: 100%;
  overflow: hidden;
  display: flex;
  flex-direction: row;
  min-height: 680px;
`;

const Content = styled.div`
  overflow-y: auto;
  width: 100%;
  height: 100%;
`;

const MainView: React.FC = (props) => {
  const { pathname } = useRouter();

  const [isAuthorized, setIsAuthorized] = useState<boolean>(false);

  // TODO: not the propersolution but works for now
  const isSidebar = !pathname.split("/").includes(RoutePaths.Payment);
  const isUpgradePlanBar = !pathname.split("/").includes(RoutePaths.Payment);

  const onUpgradePlan = () => {
    setIsAuthorized(true);
  };
  return (
    <MainContainer>
      {isSidebar && <SideBar />}
      <Content>
        <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />}>
          <React.Suspense fallback={<LoadingPage />}>
            {isAuthorized && (
              <UnauthorizedModal
                onClose={() => {
                  setIsAuthorized(false);
                }}
              />
            )}
            {isUpgradePlanBar && <UpgradePlanBar onUpgradePlan={onUpgradePlan} />}
            {props.children}
          </React.Suspense>
        </ResourceNotFoundErrorBoundary>
      </Content>
    </MainContainer>
  );
};

export default MainView;
