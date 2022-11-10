// import { faRocket } from "@fortawesome/free-solid-svg-icons";
import { faHome } from "@fortawesome/free-solid-svg-icons";
import { faGear } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classnames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";
import { NavLink } from "react-router-dom";
import styled from "styled-components";

import { Link } from "components";
import Version from "components/Version";

import { useConfig } from "config";
// import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import useRouter from "hooks/useRouter";

import { RoutePaths } from "../../../pages/routePaths";
// import ConnectionsIcon from "./components/ConnectionsIcon";
// import DestinationIcon from "./components/DestinationIcon";
// import DocsIcon from "./components/DocsIcon";
// import OnboardingIcon from "./components/OnboardingIcon";
// import SettingsIcon from "./components/SettingsIcon";
// import SidebarPopout from "./components/SidebarPopout";
// import SourceIcon from "./components/SourceIcon";
// import { NotificationIndicator } from "./NotificationIndicator";
import styles from "./SideBar.module.scss";

const Bar = styled.nav`
  width: 300px;
  min-width: 65px;
  height: 100%;
  background: ${({ theme }) => theme.white};
  padding: 23px 0px 15px 0px;
  text-align: center;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  position: relative;
  z-index: 9999;
`;

const Menu = styled.ul`
  padding: 0;
  margin: 20px 0 0;
  width: 100%;
`;

const Text = styled.div`
  margin-top: 7px;
  color: #4f46e5;
  padding-left: 10px;
  font-weight: bold;
`;

const TextSetting = styled.div`
  color: black;
  padding-left: 15px;
  font-weight: bold;
`;

const TextLogo = styled.div`
  margin-top: 7px;
  color: black;
  padding-left: 15px;
  font-size: 20px;
  font-weight: bold;
`;

const Logo = styled.div`
  margin-top: 30px;
  margin-bottom: 50px;
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  font-weight: normal;
`;

const ButtonCenter = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

const Button = styled.button`
  border: none;
  border-radius: 8px;
  margin-bottom: 50px;
  padding: 15px 30px;
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  background-color: #eae9ff;
`;

const HelpIcon = styled(FontAwesomeIcon)`
  font-size: 21px;
  line-height: 21px;
  color: #4f46e5;
`;

const SettingIcon = styled(FontAwesomeIcon)`
  font-size: 21px;
  line-height: 21px;
  color: black;
`;

export const useCalculateSidebarStyles = () => {
  const { location } = useRouter();

  const menuItemStyle = (isActive: boolean) => {
    const isChild = location.pathname.split("/").length > 4 && location.pathname.split("/")[3] !== "settings";
    return classnames(styles.menuItem, { [styles.active]: isActive, [styles.activeChild]: isChild && isActive });
  };

  return ({ isActive }: { isActive: boolean }) => menuItemStyle(isActive);
};

export const getPopoutStyles = (isOpen?: boolean) => {
  return classnames(styles.menuItem, { [styles.popoutOpen]: isOpen });
};

const SideBar: React.FC = () => {
  const config = useConfig();
  // const workspace = useCurrentWorkspace();

  const navLinkClassName = useCalculateSidebarStyles();

  return (
    <Bar>
      <div>
        <Link to="" $clear>
          <Logo>
            <img src="/daspireLogo1.svg" alt="logo" height={40} width={40} />
            <TextLogo>
              <FormattedMessage id="sidebar.DaspireLogo" />
            </TextLogo>
          </Logo>
        </Link>
        <Menu>
          <li>
            <NavLink className={navLinkClassName} to={RoutePaths.Connections}>
              <HelpIcon icon={faHome} />
              <Text>
                <FormattedMessage id="sidebar.DaspireDashboard" />
              </Text>
            </NavLink>
          </li>
          {/* {workspace.displaySetupWizard ? (*/}
          {/*  <li>*/}
          {/*    <NavLink className={navLinkClassName} to={RoutePaths.Onboarding}>*/}
          {/*      <OnboardingIcon />*/}
          {/*      <Text>*/}
          {/*        <FormattedMessage id="sidebar.onboarding" />*/}
          {/*      </Text>*/}
          {/*    </NavLink>*/}
          {/*  </li>*/}
          {/* ) : null}*/}
          {/* <li>*/}
          {/*  <NavLink className={navLinkClassName} to={RoutePaths.Connections}>*/}
          {/*    <ConnectionsIcon />*/}
          {/*    <Text>*/}
          {/*      <FormattedMessage id="sidebar.connections" />*/}
          {/*    </Text>*/}
          {/*  </NavLink>*/}
          {/* </li>*/}
          {/* <li>*/}
          {/*  <NavLink className={navLinkClassName} to={RoutePaths.Source}>*/}
          {/*    <SourceIcon />*/}
          {/*    <Text>*/}
          {/*      <FormattedMessage id="sidebar.sources" />*/}
          {/*    </Text>*/}
          {/*  </NavLink>*/}
          {/* </li>*/}
          {/* <li>*/}
          {/*  <NavLink className={navLinkClassName} to={RoutePaths.Destination}>*/}
          {/*    <DestinationIcon />*/}
          {/*    <Text>*/}
          {/*      <FormattedMessage id="sidebar.destinations" />*/}
          {/*    </Text>*/}
          {/*  </NavLink>*/}
          {/* </li>*/}
        </Menu>
      </div>
      <Menu>
        {/* <li>*/}
        {/*  <a href={config.links.updateLink} target="_blank" rel="noreferrer" className={styles.menuItem}>*/}
        {/*    <HelpIcon icon={faRocket} />*/}
        {/*    <Text>*/}
        {/*      <FormattedMessage id="sidebar.update" />*/}
        {/*    </Text>*/}
        {/*  </a>*/}
        {/* </li>*/}
        {/* <li>*/}
        {/*  <SidebarPopout options={[{ value: "docs" }, { value: "slack" }, { value: "recipes" }]}>*/}
        {/*    {({ onOpen, isOpen }) => (*/}
        {/*      <button className={getPopoutStyles(isOpen)} onClick={onOpen}>*/}
        {/*        <DocsIcon />*/}
        {/*        <Text>*/}
        {/*          <FormattedMessage id="sidebar.resources" />*/}
        {/*        </Text>*/}
        {/*      </button>*/}
        {/*    )}*/}
        {/*  </SidebarPopout>*/}
        {/* </li>*/}

        {/* <li>*/}
        {/*  <NavLink className={navLinkClassName} to={RoutePaths.Settings}>*/}
        {/*    <React.Suspense fallback={null}>*/}
        {/*      <NotificationIndicator />*/}
        {/*    </React.Suspense>*/}
        {/*    <SettingsIcon />*/}
        {/*    <Text>*/}
        {/*      <FormattedMessage id="sidebar.settings" />*/}
        {/*    </Text>*/}
        {/*  </NavLink>*/}
        {/* </li>*/}
        <li>
          <ButtonCenter>
            <Button>
              <SettingIcon icon={faGear} />
              <TextSetting>
                <FormattedMessage id="sidebar.DaspireSetting" />
              </TextSetting>
            </Button>
          </ButtonCenter>
        </li>
        {config.version ? (
          <li>
            <Version primary />
          </li>
        ) : null}
      </Menu>
    </Bar>
  );
};

export default SideBar;
