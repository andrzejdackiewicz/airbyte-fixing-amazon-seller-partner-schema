import React from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

import { ContentCard, PreferencesForm } from "components";
import useWorkspace from "components/hooks/services/useWorkspaceHook";

const SettingsCard = styled(ContentCard)`
  max-width: 638px;
  width: 100%;
`;

const Content = styled.div`
  padding: 27px 26px 15px;
`;

const AccountSettings: React.FC = () => {
  const { workspace } = useWorkspace();
  console.log(workspace);

  const onSubmit = (data: {
    email: string;
    anonymousDataCollection: boolean;
    news: boolean;
    securityUpdates: boolean;
  }) => {
    console.log(data);
  };

  return (
    <SettingsCard title={<FormattedMessage id="settings.accountSettings" />}>
      <Content>
        <PreferencesForm onSubmit={onSubmit} isEdit />
      </Content>
    </SettingsCard>
  );
};

export default AccountSettings;
