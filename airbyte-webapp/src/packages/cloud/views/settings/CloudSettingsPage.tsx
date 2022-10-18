import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

// import useConnector from "hooks/services/useConnector";
import { AccountSettingsView } from "packages/cloud/views/users/AccountSettingsView";
import { UsersSettingsView } from "packages/cloud/views/users/UsersSettingsView";
import { WorkspaceSettingsView } from "packages/cloud/views/workspaces/WorkspaceSettingsView";
import SettingsPage from "pages/SettingsPage";
import {
  DestinationsPage as SettingsDestinationPage,
  SourcesPage as SettingsSourcesPage,
} from "pages/SettingsPage/pages/ConnectorsPage";
// import ConfigurationsPage from "pages/SettingsPage/pages/ConfigurationsPage";
import NotificationPage from "pages/SettingsPage/pages/NotificationPage";
import { PageConfig } from "pages/SettingsPage/SettingsPage";
import { isOsanoActive, showOsanoDrawer } from "utils/dataPrivacy";

import { CloudSettingsRoutes } from "./routePaths";

export const CloudSettingsPage: React.FC = () => {
  // TODO: uncomment when supported in cloud
  // const { countNewSourceVersion, countNewDestinationVersion } = useConnector();

  const pageConfig = useMemo<PageConfig>(
    () => ({
      menuConfig: [
        {
          category: <FormattedMessage id="settings.userSettings" />,
          routes: [
            {
              path: CloudSettingsRoutes.Account,
              name: <FormattedMessage id="settings.account" />,
              component: AccountSettingsView,
            },
            ...(isOsanoActive()
              ? [
                  {
                    name: <FormattedMessage id="settings.cookiePreferences" />,
                    path: "__COOKIE_PREFERENCES__", // Special path with no meaning, since the onClick will be triggered
                    onClick: () => showOsanoDrawer(),
                  },
                ]
              : []),
          ],
        },
        {
          category: <FormattedMessage id="settings.workspaceSettings" />,
          routes: [
            {
              path: CloudSettingsRoutes.Workspace,
              name: <FormattedMessage id="settings.generalSettings" />,
              component: WorkspaceSettingsView,
              id: "workspaceSettings.generalSettings",
            },
            {
              path: CloudSettingsRoutes.Source,
              name: <FormattedMessage id="tables.sources" />,
              // indicatorCount: countNewSourceVersion,
              component: SettingsSourcesPage,
            },
            {
              path: CloudSettingsRoutes.Destination,
              name: <FormattedMessage id="tables.destinations" />,
              // indicatorCount: countNewDestinationVersion,
              component: SettingsDestinationPage,
            },
            // {
            //   path: CloudSettingsRoutes.Configuration,
            //   name: <FormattedMessage id="admin.configuration" />,
            //   component: ConfigurationsPage,
            // },
            {
              path: CloudSettingsRoutes.AccessManagement,
              name: <FormattedMessage id="settings.accessManagementSettings" />,
              component: UsersSettingsView,
              id: "workspaceSettings.accessManagementSettings",
            },
            {
              path: CloudSettingsRoutes.Notifications,
              name: <FormattedMessage id="settings.notifications" />,
              component: NotificationPage,
            },
          ],
        },
      ],
    }),
    []
  );

  return <SettingsPage pageConfig={pageConfig} />;
};
